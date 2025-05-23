# frozen_string_literal: true

# Released under the MIT License.
# Copyright, 2018, by Huba Nagy.
# Copyright, 2018-2024, by Samuel Williams.

require "async/redis"
require "async/redis/client"
require "async/redis/key"

require "sus/fixtures/async"

require "securerandom"

ClientContext = Sus::Shared("client context") do
	include Sus::Fixtures::Async::ReactorContext
	
	let(:endpoint) {Async::Redis.local_endpoint}
	let(:client) {@client = Async::Redis::Client.new(endpoint)}
	
	let(:root) {Async::Redis::Key["async-redis:test:#{SecureRandom.uuid}"]}
	
	before do
		keys = client.keys("#{root}:*")
		client.del(*keys) if keys.any?
	end
	
	after do
		@client&.close
	end
end
