# frozen_string_literal: true

# Released under the MIT License.
# Copyright, 2019, by David Ortiz.
# Copyright, 2019-2024, by Samuel Williams.

require "async/redis/client"
require "async/redis/context/pipeline"
require "sus/fixtures/async"
require "securerandom"

describe Async::Redis::Context::Pipeline do
	include Sus::Fixtures::Async::ReactorContext
	
	let(:endpoint) {Async::Redis.local_endpoint}
	let(:client) {Async::Redis::Client.new(endpoint)}
	let(:pool) {client.instance_variable_get(:@pool)}
	let(:pipeline) {Async::Redis::Context::Pipeline.new(pool)}
	
	let(:pairs) do
		{pipeline_key_1: "123", pipeline_key_2: "456"}
	end
	
	with "#call" do
		it "accumulates commands without running them" do
			pairs.each do |key, value|
				pipeline.call("SET", key, value)
			end
			
			pipeline.close
			
			pairs.each do |key, value|
				expect(client.get(key)).to be == value
			end
		ensure
			client.close
		end
	end
	
	with "#collect" do
		it "accumulates commands and runs them" do
			pairs.each do |key, value|
				pipeline.call("SET", key, value)
			end
			
			pipeline.flush
			
			pairs.each do |key, value|
				pipeline.call("GET", key)
			end
			
			expect(pipeline.collect).to be == pairs.values
		ensure
			pipeline.close
		end
	end
	
	with "an interrupted pipeline" do
		# Use a single connection so that any reuse is observable:
		let(:client) {Async::Redis::Client.new(endpoint, limit: 1)}
		let(:other) {Async::Redis::Client.new(endpoint)}
		let(:key) {"async-redis:test:#{SecureRandom.uuid}:list"}
		
		it "does not reuse the connection" do
			# Establish the connection so that the request is sent without yielding:
			client.call("PING")
			
			task = Async do
				client.pipeline do |context|
					context.call("BLPOP", key, 1)
					context.collect
				end
			end
			
			# Stop the pipeline while it is waiting for a response:
			task.stop
			
			# The response to the interrupted request is sent to its connection:
			other.call("RPUSH", key, "stale")
			
			expect(client.call("PING")).to be == "PONG"
		ensure
			other.call("DEL", key)
			other.close
			client.close
		end
	end
end
