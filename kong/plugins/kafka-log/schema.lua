local types = require "kong.plugins.kafka-log.types"

--- Validates value of `bootstrap_servers` field.
local function check_bootstrap_servers(values)
  if values and 0 < #values then
    for _, value in ipairs(values) do
      local server = types.bootstrap_server(value)
      if not server then
        return false, "invalid bootstrap server value: " .. value
      end
    end
    return true
  end
  return false, "bootstrap_servers is required"
end

return {
  fields = {
    bootstrap_servers = { type = "array", required = true, func = check_bootstrap_servers },
    topic = { type = "string", required = true },
    timeout = { type = "number", default = 10000 },
    keepalive = { type = "number", default = 60000 },
    producer_request_acks = { type = "number", default = 1, enum = { -1, 0, 1 } },
    producer_request_timeout = { type = "number", default = 4000 },
    producer_request_limits_messages_per_request = { type = "number", default = 5 },
    producer_request_limits_bytes_per_request = { type = "number", default = 10000000 },
    producer_request_retries_max_attempts = { type = "number", default = 5 },
    producer_request_retries_backoff_timeout = { type = "number", default = 100 },
    producer_async = { type = "boolean", default = true },
    producer_async_flush_timeout = { type = "number", default = 1000 },
    producer_async_buffering_limits_messages_in_memory = { type = "number", default = 5000 },
    encrypted = { type = "boolean", default = true },
    compressed = { type = "boolean", default = true },
    encryption_key = { type = "string", default = "" },
    log_request_body = { type = "boolean", default = true },
    log_response_body = { type = "boolean", default = true }
  }
}
