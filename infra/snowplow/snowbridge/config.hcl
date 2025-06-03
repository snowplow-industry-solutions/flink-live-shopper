license {
  accept = env("ACCEPT_LICENSE")
}

source {
  use "kinesis" {
    stream_name       = "enriched-good"
    region            = "eu-west-2"
    app_name          = "snowbridge"
    concurrent_writes = 15
    custom_aws_endpoint = "http://localhost.localstack.cloud:4566"
    read_throttle_delay_ms = 500
    start_timestamp = env("START_TIMESTAMP")
  }
}

transform {
  use "spEnrichedToJson" {}
}

transform {
  use "js" {
    script_path   = env.JS_SCRIPT_PATH
    snowplow_mode = true
  }
}

target {
  use "kafka" {
    brokers    = "kafka:9094"
    topic_name = "snowplow-enriched-good"
  }
}

failure_target {
    use "stdout" {}
}

// log level configuration (default: "info")
log_level = "info"
disable_telemetry = true