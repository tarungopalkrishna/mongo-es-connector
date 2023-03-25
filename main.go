package main

// https://www.mongodb.com/community/forums/t/change-stream-projection-getting-last-resume-token/2817/3
// Be careful when the database is sharded
// If the deployment is a sharded cluster, a shard removal may cause an open change stream cursor to close, and the closed change stream cursor may not be fully resumable.

// How can I make this service distributed?
/*
CHATGPT:
There are a few different ways you could ensure that only one replica of your service is streaming data from Mongo
and updating Elasticsearch at a time when you scale up the replicas in Kubernetes:

1. Use a leader election mechanism: This would involve having each replica of your service compete
   to become the leader, and only the leader would be responsible for streaming data from Mongo and
   updating Elasticsearch. This can be done using Kubernetes primitives like ConfigMaps or using a
   separate leader election library like ZooKeeper.

2. Use a shared lock: This would involve having each replica of your service acquire a lock
   (e.g. using a distributed key-value store like etcd) before streaming data from Mongo and
   updating Elasticsearch. Only one replica would be able to acquire the lock at a time,
   ensuring that only one replica is streaming data at a time.

3. Use a shared queue: One replica can stream data and put it in a shared queue. Other replicas
   can dequeue it and update elasticsearch.

4. Use a shared state: You can store a shared state in a shared kubernetes configMap, that can be
   updated by any replica and read by all replicas. If the shared state indicates that streaming
   is already in progress, a new replica can wait for the streaming to complete before starting to
   stream data.

5. Use Kubernetes Pod Anti-Affinity: This would configure the Kubernetes scheduler to avoid scheduling
   multiple replicas of your service on the same node. This would ensure that only one replica would be
   running on each node, and therefore only one replica would be streaming data from Mongo and updating
   Elasticsearch.
*/

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"sync"
	"time"

	etcdClient "go.etcd.io/etcd/client/v3"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v3"

	// This is for thread safe queue
	"github.com/enriquebris/goconcurrentqueue"
	// This is for elastic search go driver
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

type SericeConfig struct {
	// MongoDb
	MongoDbUri string
	MongoDbDb  string
	MongoDbCol string
	Project    bson.D
	CsProject  bson.D
	Filter     bson.D
	Sort       bson.D
	// ElasticSearch
	EsCloudId    string
	EsApiKey     string
	EsIndexName  string
	InstanceName string
	DataMapping  map[string]string
}

type ChangeStreamOperation struct {
	DocumentId    primitive.ObjectID `bson:"_id"`
	OperationType string
	FullDocument  map[string]interface{}
	Ns            struct {
		db   string
		coll string
	}
	UpdateDescription struct {
		updatedFields map[string]interface{}
		removedFields map[string]interface{}
	}
	ResumeToken string
	// Not supprting this as of now, will see in future releases
	// clusterTime  time.Time
}

var CurrentResumeToken string

var _change_stream_lock bool = false

func acquireLock() {
	log.Println("Acquiring lock")
	_change_stream_lock = true
}

func releaseLock() {
	log.Println("Releasing lock")
	_change_stream_lock = false
}

func isLocked() bool {
	return _change_stream_lock
}

// Function to read data from mongodb
func loadIntialData(coll *mongo.Collection, es *elasticsearch.Client, serviceConfig SericeConfig, client *etcdClient.Client) {

	// TODO: Need to write a failover for this part of the code
	// how do I handle this for bulk of data?
	// Assuming right now this part of the code runs without exceptions

	// Check status of inital data upload
	if initStatus, err := client.Get(context.Background(), "mongo-es-init-stage"); err == nil {
		if string(initStatus.Kvs[0].Value) == "done" {
			log.Println("Initial data already loaded")
			releaseLock()
		}
	} else if errors.Is(err, os.ErrNotExist) {

		// Reacquiring the lock just in case
		// Acquire lock
		acquireLock()

		value, err := client.Put(context.Background(), "mongo-es-init-stage", "running")
		if err != nil {
			log.Fatal(err)
		} else {
			log.Println(value)
		}

		var cursor *mongo.Cursor
		var results []bson.M

		opts := options.Find().SetSort(serviceConfig.Sort).SetProjection(serviceConfig.Project)
		// Do I need to check if createAt exists?
		cursor, find_err := coll.Find(context.TODO(), serviceConfig.Filter, opts)
		if find_err == mongo.ErrNoDocuments {
			fmt.Print("No documents found")
			// Debate on which is better writing this line twice or defer wg.Done()?
			wg.Done()
			return
		}
		if find_err != nil {
			panic(find_err)
		}

		if err := cursor.All(context.TODO(), &results); err != nil {
			panic(err)
		}
		for _, result := range results {
			// TODO: Make each request a goroutine : https://pkg.go.dev/github.com/elastic/go-elasticsearch#section-readme
			res, _ := json.Marshal(result)

			req := esapi.IndexRequest{
				Index: serviceConfig.EsIndexName,
				// IMP: The Id of the docuemnt is used as unique identifier
				DocumentID: result["id"].(primitive.ObjectID).Hex(),
				Body:       bytes.NewReader(res),
				Refresh:    "true",
			}

			do_res, err := req.Do(context.Background(), es)
			if err != nil {
				log.Fatalf("Error getting response: %s", err)
			}
			defer do_res.Body.Close()

			if do_res.IsError() {
				log.Printf("[%s] Error indexing document ID=%s", do_res.Status(), string(result["id"].(primitive.ObjectID).Hex()))
			} else {
				// Deserialize the response into a map.
				var r map[string]interface{}
				if err := json.NewDecoder(do_res.Body).Decode(&r); err != nil {
					log.Printf("Error parsing the response body: %s", err)
				} else {
					// Print the response status and indexed document version.
					log.Printf("[%s] %s; version=%d", do_res.Status(), r["result"], int(r["_version"].(float64)))
					client.Put(context.Background(), "mongo-es-init-state", string(result["id"].(primitive.ObjectID).Hex()))
				}
			}
		}

		// Update a persistent variable to indicate that the initial data has been loaded
		// TODO: Need to update like a redis or a etcd or something
		// Writing this to a file for now, wont work when running in a container
		// Create a file by the name initial_data_loaded.lock

		// Release lock
		releaseLock()

		d_value, d_err := client.Put(context.Background(), "mongo-es-init-stage", "done")
		if err != nil {
			log.Fatal(d_err)
		} else {
			log.Println(d_value)
		}

		log.Print("Uploaded the initial data!\n")
	}
	wg.Done()
}

// WARN: If ever this function is parallelized, then the lock needs to be handled properly
// Updation of the resume token needs to be handled - https://docs.mongodb.com/manual/changeStreams/#change-stream-resume-token
// Need to determine which event is latest when updating the resume token when parallelized - interesting problem
func uploadToElasticSearch(q *goconcurrentqueue.FIFO, es *elasticsearch.Client, serviceConfig SericeConfig, client *etcdClient.Client) {
	for {
		if isLocked() {
			continue
		}
		item, err := q.Dequeue()
		if err != nil {
			continue
		}
		cs_op, ok := item.(ChangeStreamOperation)
		if !ok {
			// This is a debug statement
			log.Println("Not a ChangeStreamOperation")
			log.Print("Is of data type:")
			log.Println(reflect.TypeOf(cs_op))
			continue
		}

		// Make each request a goroutine : https://pkg.go.dev/github.com/elastic/go-elasticsearch#section-readme
		// For an insert operation the following in returned
		// Keys: ['_id', 'operationType', 'clusterTime', 'fullDocument', 'ns', 'documentKey']
		if cs_op.OperationType == "insert" {
			log.Println("Inserting the data in elastic search")
			res, _ := json.Marshal(cs_op.FullDocument)

			req := esapi.IndexRequest{
				Index:      serviceConfig.EsIndexName,
				DocumentID: cs_op.DocumentId.Hex(),
				Body:       bytes.NewReader(res),
				Refresh:    "true",
			}

			do_res, err := req.Do(context.Background(), es)
			if err != nil {
				log.Fatalf("Error getting response: %s", err)
			}
			defer do_res.Body.Close()

			if do_res.IsError() {
				log.Printf("[%s] Error indexing document ID=%s", do_res.Status(), string(cs_op.DocumentId.Hex()))
			} else {
				// Deserialize the response into a map.
				var r map[string]interface{}
				if err := json.NewDecoder(do_res.Body).Decode(&r); err != nil {
					log.Printf("Error parsing the response body: %s", err)
				} else {
					// Print the response status and indexed document version.
					log.Printf("[%s] %s; version=%d", do_res.Status(), r["result"], int(r["_version"].(float64)))
				}
			}

			CurrentResumeToken = cs_op.ResumeToken
		}
		// For an update operation the following in returned
		// Keys: ['_id', 'operationType', 'clusterTime', 'ns', 'documentKey', 'updateDescription']
		// Update the data in elastic search
		// Can raise an not found exception if the document is not found in elastic search
		// elasticsearch.NotFoundError -> catch this
		if cs_op.OperationType == "update" {
			log.Println("Updating the data in elastic search")
			// Update the data in elastic search
			log.Println(cs_op.FullDocument)
			res, s := json.Marshal(cs_op.FullDocument)
			// u := User{FirstName: "test", LastName: "test", Description: "test", Title: "test", _id: cs_op.DocumentId.Hex(), IsActive: true, IsDeleted: false}
			// res, s := json.Marshal(u)
			if s != nil {
				log.Println("Error in marshalling the data")
				log.Println(s)
				continue
			}
			// This step is vv imp - took me a day to figure this out
			res = []byte(`{"doc":` + string(res) + `}`)

			req := esapi.UpdateRequest{
				Index:      serviceConfig.EsIndexName,
				DocumentID: cs_op.DocumentId.Hex(),
				Body:       bytes.NewReader(res),
				Refresh:    "true",
				// ErrorTrace: true,
			}


			do_res, err := req.Do(context.Background(), es)
			if err != nil {
				log.Fatalf("Error getting response: %s", err)
			}
			// TODO: understand what defer actually does
			defer do_res.Body.Close()

			if do_res.IsError() {
				log.Println(do_res.String())
				log.Println(do_res.Body)
				log.Println(do_res.HasWarnings())
				log.Println(do_res.Warnings())
				log.Printf("[%s] Error indexing document ID=%s", do_res.Status(), cs_op.DocumentId.Hex())
				log.Println(do_res.Header)
			} else {
				// Deserialize the response into a map.
				var r map[string]interface{}
				if err := json.NewDecoder(do_res.Body).Decode(&r); err != nil {
					log.Printf("Error parsing the response body: %s", err)
				} else {
					// Print the response status and indexed document version.
					log.Printf("[%s] %s; version=%d", do_res.Status(), r["result"], int(r["_version"].(float64)))
				}
			}

			CurrentResumeToken = cs_op.ResumeToken

		}

		// For a delete operation the following in returned
		// Keys: ['_id', 'operationType', 'clusterTime', 'ns', 'documentKey']
		// Delete the data from elastic search
		if cs_op.OperationType == "delete" {
			log.Fatalln("Deleting the data from elastic search - FEATURE NOT IMPLEMENTED YET")
		}

		// Update the resume token in etcd
		client.Put(context.Background(), "mongo-es-stream-resume-token", cs_op.ResumeToken)

	}
}

func watchChanges(coll *mongo.Collection, es *elasticsearch.Client, serviceConfig SericeConfig, client *etcdClient.Client) {

	queue := goconcurrentqueue.NewFIFO()

	pipeline := mongo.Pipeline{
		bson.D{{"$match",
			bson.D{{"operationType",
				bson.D{{"$in",
					bson.A{"insert", "update", "delete"},
				},
				},
			},
			},
		},
		}, bson.D{{"$project",
			serviceConfig.CsProject,
		},
		},
	}

	// Add this to the pipeline
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	cs, err := coll.Watch(context.TODO(), pipeline, opts)
	if err != nil {
		panic(err)
	}

	defer cs.Close(context.TODO())

	go uploadToElasticSearch(queue, es, serviceConfig, client)

	log.Println("Waiting For Change Events")
	for cs.Next(context.TODO()) {
		var event bson.M
		if err := cs.Decode(&event); err != nil {
			panic(err)
		}
		// output, err := json.MarshalIndent(event, "", "    ")
		// if err != nil {
		// 	panic(err)
		// }
		// fmt.Printf("%s\n", output)

		var each_request ChangeStreamOperation
		// var u User
		var ok bool

		// Kyes: ['_id', 'operationType', 'clusterTime', 'ns', 'documentKey', 'fullDocument']
		// Keys: ['_id', 'operationType', 'clusterTime', 'ns', 'documentKey', 'updateDescription']
		// Keys: ['_id', 'operationType', 'clusterTime', 'ns', 'documentKey']

		each_request.OperationType, ok = event["operationType"].(string)
		if !ok {
			log.Println("Error in operationType")
			continue
		}
		// each_request.clusterTime, ok = event["clusterTime"].(primitive.Timestamp)
		each_request.Ns.db, ok = event["ns"].(primitive.M)["db"].(string)
		if !ok {
			log.Println("Error no db in ns")
			continue
		}
		each_request.Ns.coll, ok = event["ns"].(primitive.M)["coll"].(string)
		if !ok {
			log.Println("Error no coll in ns")
			continue
		}
		each_request.DocumentId, ok = event["documentKey"].(primitive.M)["_id"].(primitive.ObjectID)
		each_request.ResumeToken = event["_id"].(primitive.M)["_data"].(string)

		if event["operationType"] == "insert" {
			each_request.FullDocument = event["fullDocument"].(primitive.M)
		}
		if event["operationType"] == "update" {
			each_request.FullDocument = event["fullDocument"].(primitive.M)
		}
		// TODO: Will handle this case later as it is currently not a feature in the database
		if event["operationType"] == "delete" {
			log.Print("Not handling delete events yet")
			continue
		}

		// Add to queue
		queue.Enqueue(each_request)
	}
	if err := cs.Err(); err != nil {
		panic(err)
	}

	wg.Done()
}

func etcdInit(client *etcdClient.Client) {

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	data, err := client.Get(ctx, "mongo-es-connector-state")
	if err != nil {
		// handle error!
		// with etcd clientv3 <= v3.3
		if err == context.Canceled {
			// grpc balancer calls 'Get' with an inflight client.Close
		} else if err == grpc.ErrClientConnClosing { // <= gRCP v1.7.x
			// grpc balancer calls 'Get' after client.Close.
		}
		// with etcd clientv3 >= v3.4
		if etcdClient.IsConnCanceled(err) {
			// gRPC client connection is closed
		}
	}

	cancel() // Should this be here?

	if data.Kvs == nil {
		log.Println("Init data not set in etcd")
		return
	} else {
		log.Println(string(data.Kvs[0].Value))
	}
}

func loadConfig(content_type string) *map[string]interface{} {
	// cfg := make(map[interface{}]interface{})
	// ChatGPT says this, what does it mean?
	// In the loadConfig function, the cfg variable is declared as a
	// map[string]interface{}, but is not initialized. This means that
	// it will be nil when passed to yaml.Unmarshal, causing a runtime
	// qpanic. You should initialize the map before passing it to yaml.
	// Unmarshal, for example by using cfg := make(map[string]interface{}).

	// Also need to understand the scope of cfg
	cfg := make(map[string]interface{})
	// var cfg map[string]interface{}
	cfgFile, err := os.Open(".config/config.yaml")
	if err != nil {
		log.Fatal(err)
	}
	defer cfgFile.Close()
	byteValue, _ := ioutil.ReadAll(cfgFile)
	err = yaml.Unmarshal(byteValue, &cfg)
	if err != nil {
		log.Fatal(err)
	}
	if cfg[content_type] == nil {
		log.Fatal("No config found for content type:", content_type)
		panic("No config found for content type:" + content_type)
	}
	x := cfg[content_type].(map[string]interface{})

	return &x
	// cfg = cfg[content_type].([]interface{})
	// return &cfg
}

var wg sync.WaitGroup

func main() {

	content_service_name := os.Getenv("CONTENT_SERVICE_NAME")
	if content_service_name == "" {
		log.Fatal("You must set your 'CONTENT_SERVICE_NAME' environmental variable.")
	}
	// I'm returning a pointer to a map
	streamCfg := *loadConfig(content_service_name)
	log.Println(streamCfg)
	// streamCfg := streamCfg.(map[string]interface{}) // Find a better hack for this
	dbName := streamCfg["database"].(string)
	collName := streamCfg["collection"].(string)
	log.Println("Connecting to database:", dbName, "and collection:", collName)
	instanceName := streamCfg["instanceName"].(string)
	dataMapper := streamCfg["data"].(map[string]interface{}) // Why can I not convert this to map[string]string?
	log.Println(dataMapper)
	dataMapping := make(map[string]string)
	for k, v := range dataMapper {
		dataMapping[k] = v.(string)
	}

	// Code to print the keys
	// keys := make([]interface{}, 0, len(somethingCfg))
	// for k := range somethingCfg {
	// 	keys = append(keys, k)
	// }
	// log.Println(keys)

	index_name := os.Getenv("INDEX_NAME")
	if index_name == "" {
		log.Fatal("You must set your 'INDEX_NAME' environmental variable.")
	}
	uri := os.Getenv("MONGODB_URI")
	if uri == "" {
		log.Fatal("You must set your 'MONGODB_URI' environmental variable. See\n\t https://www.mongodb.com/docs/drivers/go/current/usage-examples/#environment-variable")
	}
	es_cloud_id := os.Getenv("ES_CLOUD_ID")
	if es_cloud_id == "" {
		log.Fatal("You must set your 'ES_CLOUD_ID' environmental variable.")
	}
	es_api_key := os.Getenv("ES_API_KEY")
	if es_api_key == "" {
		log.Fatal("You must set your 'ES_API_KEY' environmental variable.")
	}

	mongoProject := bson.D{}
	mongoCsProject := bson.D{}
	for key, value := range dataMapping {
		mongoProject = append(mongoProject, bson.E{value, "$" + key})
		mongoCsProject = append(mongoCsProject, bson.E{"fullDocument." + value, "$fullDocument." + key})
	}

	// Exclude _id
	mongoProject = append(mongoProject, bson.E{"_id", 0})

	// Include the following parameters
	mongoCsProject = append(mongoCsProject, bson.E{"documentKey", 1})
	mongoCsProject = append(mongoCsProject, bson.E{"ns", 1})
	mongoCsProject = append(mongoCsProject, bson.E{"operationType", 1})
	mongoCsProject = append(mongoCsProject, bson.E{"updateDescription", 1})

	mongoFilter := bson.D{}
	if streamCfg["filter"] != nil {
		filter := streamCfg["filter"].(map[string]interface{})
		for key, value := range filter {
			mongoFilter = append(mongoFilter, bson.E{key, value})
		}
	}

	mongoSort := bson.D{}
	if streamCfg["sort"] != nil {
		sort := streamCfg["sort"].(map[string]interface{})
		for key, value := range sort {
			mongoSort = append(mongoSort, bson.E{key, value})
		}
	}

	serverCfg := SericeConfig{
		MongoDbUri:   uri,
		MongoDbDb:    dbName,
		MongoDbCol:   collName,
		EsCloudId:    es_cloud_id,
		EsApiKey:     es_api_key,
		EsIndexName:  index_name,
		InstanceName: instanceName,
		DataMapping:  dataMapping,
		Project:      mongoProject,
		CsProject:    mongoCsProject,
		Filter:       mongoFilter,
		Sort:         mongoSort,
	}
	// log.Println("Server config:")
	// log.Println(serverCfg)
	// log.Println("****************************************")
	acquireLock()

	client, find_err := mongo.Connect(context.TODO(), options.Client().ApplyURI(uri))
	if find_err != nil {
		panic(find_err)
	}

	client.Ping(context.TODO(), nil)
	// response := client.Ping(context.TODO(), nil)
	// log.Println(response)
	// if err != nil {
	// panic(err)
	// }

	// How does this work?
	defer func() {
		if err := client.Disconnect(context.TODO()); err != nil {
			panic(err)
		}
	}()

	// Connection to etcd
	etcd, err := etcdClient.New(etcdClient.Config{
		Endpoints:   []string{"http://localhost:2379"}, // TODO: Make this configurable
		DialTimeout: 2 * time.Second,
	})
	if err != nil {
		// etcd clientv3 >= v3.2.10, grpc/grpc-go >= v1.7.3
		if err == context.DeadlineExceeded {
			// handle errors
		}

		// etcd clientv3 <= v3.2.9, grpc/grpc-go <= v1.2.1
		if err == grpc.ErrClientConnTimeout {
			// handle errors
		}
	}

	etcdInit(etcd)

	coll := client.Database(dbName).Collection(collName)

	// Connect to elastic search
	esCfg := elasticsearch.Config{
		CloudID: es_cloud_id,
		APIKey:  es_api_key,
	}
	es, es_err := elasticsearch.NewClient(esCfg)
	if es_err != nil {
		panic(es_err)
	}
	res, err := es.Info()
	if err != nil {
		panic(err)
	}
	log.Println(res)
	log.Printf("Elastic Client Version: %s", elasticsearch.Version)
	// log.Printf("Server: %s", r["version"].(map[string]interface{})["number"])

	// TODO: Uncomment this later after understanding defer
	// defer res.Body.Close()

	wg.Add(2)

	// This below function I can change it to be run on the main thread also - can I?
	go watchChanges(coll, es, serverCfg, etcd)
	// Should I add a delay here just make sure the changes have started streaming?
	go loadIntialData(coll, es, serverCfg, etcd)

	wg.Wait()
}
