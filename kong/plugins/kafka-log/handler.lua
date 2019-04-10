local BasePlugin = require "kong.plugins.base_plugin"
local basic_serializer = require "kong.plugins.kafka-log.serializer"
local producers = require "kong.plugins.kafka-log.producers"
local util = require "kong.tools.utils"
local zlib = require "zlib"
local nacl = require "luatweetnacl"
local cjson = require "cjson"
local cjson_encode = cjson.encode

local KafkaLogHandler = BasePlugin:extend()

KafkaLogHandler.PRIORITY = 4
KafkaLogHandler.VERSION = "0.1.0"

local mt_cache = { __mode = "k" }
local producers_cache = setmetatable({}, mt_cache)

local function get_request_body()
  ngx.req.read_body()
  return ngx.req.get_body_data()
end

local function encrypt_payload(encryption_key, payload)
  local nonce = util.get_rand_bytes(24)
  return nonce .. nacl.secretbox(payload, nonce, encryption_key)
end

local function kafka_message_frame(conf, message)
  local payload = cjson_encode(message)

  if conf.compressed then
    payload = zlib.deflate(zlib.BEST_COMPRESSION, 15+16)(payload, "finish")
  end

  if conf.encrypted then
    payload = encrypt_payload(conf.encryption_key, payload)
  end

  payload = ngx.encode_base64(payload)

  frame = {
    encrypted = conf.encrypted,
    compressed = conf.compressed,
    payload = payload
  }
  return cjson_encode(frame)
end

--- Computes a cache key for a given configuration.
local function cache_key(conf)
  -- here we rely on validation logic in schema that automatically assigns a unique id
  -- on every configuartion update
  return ngx.md5(cjson_encode(conf))
end

--- Publishes a message to Kafka.
-- Must run in the context of `ngx.timer.at`.
local function log(premature, conf, message)
  if premature then
    return
  end

  local cache_key = cache_key(conf)
  if not cache_key then
    ngx.log(ngx.ERR, "[kafka-log] cannot log a given request because cache_key is invalid")
    return
  end

  local producer = producers_cache[cache_key]
  if not producer then
    local err
    producer, err = producers.new(conf)
    if not producer then
      ngx.log(ngx.ERR, "[kafka-log] failed to create a Kafka Producer for a given configuration: ", err)
      return
    end

    producers_cache[cache_key] = producer
  end

  local ok, err = producer:send(conf.topic, nil, kafka_message_frame(conf, message))
  if not ok then
    ngx.log(ngx.ERR, "[kafka-log] failed to send a message on topic ", conf.topic, ": ", err)
    return
  end
end

function KafkaLogHandler:new()
  KafkaLogHandler.super.new(self, "kafka-log")
end

function KafkaLogHandler:access(conf)
  KafkaLogHandler.super.access(self)
  ngx.ctx.http_log_extended = { req_body = "", res_body = "" }

  if (conf.log_request_body) then
      ngx.ctx.http_log_extended = { req_body = get_request_body() }
  end
end

function KafkaLogHandler:body_filter(conf)
  KafkaLogHandler.super.body_filter(self)
  if (conf.log_response_body) then
    local chunk = ngx.arg[1]
    local ctx = ngx.ctx
    local res_body = ctx.http_log_extended and ctx.http_log_extended.res_body or ""
    res_body = res_body .. (chunk or "")
    ctx.http_log_extended.res_body = res_body
  end
end

function KafkaLogHandler:log(conf, other)
  KafkaLogHandler.super.log(self)

  local message = basic_serializer.serialize(ngx)
  local ok, err = ngx.timer.at(0, log, conf, message)
  if not ok then
    ngx.log(ngx.ERR, "[kafka-log] failed to create timer: ", err)
  end
end

return KafkaLogHandler
