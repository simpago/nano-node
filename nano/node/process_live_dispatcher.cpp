#include <nano/lib/blocks.hpp>
#include <nano/node/block_processor.hpp>
#include <nano/node/process_live_dispatcher.hpp>
#include <nano/node/scheduler/priority.hpp>
#include <nano/node/vote_cache.hpp>
#include <nano/node/websocket.hpp>
#include <nano/secure/common.hpp>
#include <nano/secure/ledger.hpp>
#include <nano/secure/transaction.hpp>
#include <nano/store/component.hpp>

nano::process_live_dispatcher::process_live_dispatcher (nano::ledger & ledger, nano::scheduler::priority & scheduler, nano::vote_cache & vote_cache, nano::websocket_server & websocket) :
	ledger{ ledger },
	scheduler{ scheduler },
	vote_cache{ vote_cache },
	websocket{ websocket }
{
}

void nano::process_live_dispatcher::connect (nano::block_processor & block_processor)
{
}

void nano::process_live_dispatcher::inspect (nano::block_status const & result, nano::block const & block, secure::transaction const & transaction)
{
}

void nano::process_live_dispatcher::process_live (nano::block const & block, secure::transaction const & transaction)
{
}
