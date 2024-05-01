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
  host           = "10.216.0.3"
  username       = "sonic"
  password       = "QfLiDQpA0X9jiw3V"
}
/*

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
*/
/*
resource "clickhouse_table" "t2" {
  database      = "default"
  name          = "replicated_test"
  engine        = "ReplicatedReplacingMergeTree"
  engine_params = []
  cluster = "main"
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
  order_by = ["event_date"]
}
*/

/*
resource "clickhouse_view" "test_view" {
  database      = "default"
  name          = "test_view"
  cluster="main"
query = "select * from nx_refunds limit 10"
}
*/

resource "clickhouse_view" "test_materialized_view" {
  database      = "default"
  name          = "test_materialized_view"
  materialized = true
  to_table = "test_view"
  cluster="main"
query = "select * from default.nx_refunds limit 10"
}

