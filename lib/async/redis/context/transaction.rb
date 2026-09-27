# frozen_string_literal: true

# Released under the MIT License.
# Copyright, 2018, by Huba Nagy.
# Copyright, 2018-2025, by Samuel Williams.

require_relative "pipeline"

module Async
	module Redis
		module Context
			# Context for Redis transaction operations using MULTI/EXEC.
			class Transaction < Pipeline
				# Initialize a new transaction context.
				# @parameter pool [Pool] The connection pool to use.
				# @parameter arguments [Array] Additional arguments for the transaction.
				def initialize(pool, *arguments)
					super(pool)
					
					@active = false
				end
				
				# Begin a transaction block.
				def multi
					call("MULTI")
					
					@active = true
				end
				
				# Watch keys for changes during the transaction.
				# @parameter keys [Array(String)] The keys to watch.
				def watch(*keys)
					sync.call("WATCH", *keys)
				end
				
				# Execute all queued commands, provided that no watched keys have been modified. It's important to note that even when a command fails, all the other commands in the queue are processed – Redis will not stop the processing of commands.
				def execute
					sync.call("EXEC")
				ensure
					@active = false
				end
				
				# Discard all queued commands in the transaction.
				def discard
					sync.call("DISCARD")
				ensure
					@active = false
				end
				
				# Close the transaction, discarding it if it was neither executed nor discarded, so that the connection is not returned to the pool in the middle of a transaction.
				def close
					if @active and !@connection.closed?
						discard
					end
				ensure
					super
				end
			end
		end
	end 
end
