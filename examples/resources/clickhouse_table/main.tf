terraform {
  required_providers {
    clickhouse = {
      version = "2.0.0"
      source  = "hashicorp.com/ivanofthings/clickhouse"
    }
  }
}

provider "clickhouse" {
  port = 9000
  host           = "stg.sonic-cluster.internal.whaledb.io"
  username       = "sonic"
  password       = ""
}

resource "clickhouse_db" "test_db_clustered" {
  name    = "awesome_database"
  comment = "This is an awesome database"
  cluster = "'{cluster}'"
}


resource "clickhouse_table" "replicated_table" {
  database      = "default"
  name          = "kafka_test"
  engine        = "Kafka"
  engine_params  = ["'sonic-cluster-kafka-bootstrap.internal.sonicwhale.io:9092'", "'test'", "'test'", "'JSONEachRows'"]
  column {
    name = "event_date"
    type = "Date"
  }
  column {
    name = "event_type"
    type = "Int32"
  }
  column {
    name = "article_id"
    type = "Int32"
  }
  column {
    name = "title"
    type = "String"
  }
  
  settings = {
    kafka_thread_per_consumer = "1"
    kafka_num_consumers = "8"
  }
  
}
