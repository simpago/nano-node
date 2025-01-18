#include <nano/lib/blocks.hpp>
#include <nano/lib/logging.hpp>
#include <nano/lib/stats.hpp>
#include <nano/lib/tomlconfig.hpp>
#include <nano/node/bootstrap/bootstrap_service.hpp>
#include <nano/node/bootstrap/frontier_scan.hpp>
#include <nano/test_common/system.hpp>
#include <nano/test_common/testutil.hpp>

#include <gtest/gtest.h>

#include <sstream>

using namespace std::chrono_literals;

namespace
{
struct test_context
{
	nano::stats stats;
	nano::frontier_scan_config config;
	nano::bootstrap::frontier_scan frontier_scan;

	explicit test_context (nano::frontier_scan_config config_a = {}) :
		stats{ nano::default_logger () },
		config{ config_a },
		frontier_scan{ config, stats }
	{
	}
};
}

TEST (bootstrap_frontier_scan, construction)
{
	test_context ctx{};
	auto & frontier_scan = ctx.frontier_scan;
}

TEST (bootstrap_frontier_scan, next_basic)
{
	nano::frontier_scan_config config;
	config.head_parallelism = 2; // Two heads for simpler testing
	config.consideration_count = 3;
	test_context ctx{ config };
	auto & frontier_scan = ctx.frontier_scan;

	// First call should return first head, account number 1 (avoiding burn account 0)
	auto first = frontier_scan.next ();
	ASSERT_EQ (first.number (), 1);

	// Second call should return second head, account number 0x7FF... (half the range)
	auto second = frontier_scan.next ();
	ASSERT_EQ (second.number (), nano::account{ "7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF" });

	// Third call should return first head again, sequentially iterating through heads
	auto third = frontier_scan.next ();
	ASSERT_EQ (third.number (), 1);
}

TEST (bootstrap_frontier_scan, process_basic)
{
	nano::frontier_scan_config config;
	config.head_parallelism = 1; // Single head for simpler testing
	config.consideration_count = 3;
	config.candidates = 5;
	test_context ctx{ config };
	auto & frontier_scan = ctx.frontier_scan;

	// Get initial account to scan
	auto start = frontier_scan.next ();
	ASSERT_EQ (start.number (), 1);

	// Create response with some frontiers
	std::deque<std::pair<nano::account, nano::block_hash>> response;
	response.push_back ({ nano::account{ 2 }, nano::block_hash{ 1 } });
	response.push_back ({ nano::account{ 3 }, nano::block_hash{ 2 } });

	// Process should not be done until consideration_count is reached
	ASSERT_FALSE (frontier_scan.process (start, response));
	ASSERT_FALSE (frontier_scan.process (start, response));

	// Head should not advance before reaching `consideration_count` responses
	ASSERT_EQ (frontier_scan.next (), 1);

	// After consideration_count responses, should be done
	ASSERT_TRUE (frontier_scan.process (start, response));

	// Head should advance to next account and start subsequent scan from there
	ASSERT_EQ (frontier_scan.next (), 3);
}

TEST (bootstrap_frontier_scan, range_wrap_around)
{
	nano::frontier_scan_config config;
	config.head_parallelism = 1;
	config.consideration_count = 1;
	config.candidates = 1;
	test_context ctx{ config };
	auto & frontier_scan = ctx.frontier_scan;

	auto start = frontier_scan.next ();

	// Create response that would push next beyond the range end
	std::deque<std::pair<nano::account, nano::block_hash>> response;
	response.push_back ({ nano::account{ std::numeric_limits<nano::uint256_t>::max () }, nano::block_hash{ 1 } });

	// Process should succeed and wrap around
	ASSERT_TRUE (frontier_scan.process (start, response));

	// Next account should be back at start of range
	auto next = frontier_scan.next ();
	ASSERT_EQ (next.number (), 1);
}

TEST (bootstrap_frontier_scan, cooldown)
{
	nano::frontier_scan_config config;
	config.head_parallelism = 1;
	config.consideration_count = 1;
	config.cooldown = 250ms;
	test_context ctx{ config };
	auto & frontier_scan = ctx.frontier_scan;

	// First call should succeed
	auto first = frontier_scan.next ();
	ASSERT_NE (first.number (), 0);

	// Immediate second call should fail (return 0)
	auto second = frontier_scan.next ();
	ASSERT_EQ (second.number (), 0);

	// After cooldown, should succeed again
	std::this_thread::sleep_for (500ms);
	auto third = frontier_scan.next ();
	ASSERT_NE (third.number (), 0);
}

TEST (bootstrap_frontier_scan, candidate_trimming)
{
	nano::frontier_scan_config config;
	config.head_parallelism = 1;
	config.consideration_count = 2;
	config.candidates = 3; // Only keep the lowest candidates
	test_context ctx{ config };
	auto & frontier_scan = ctx.frontier_scan;

	auto start = frontier_scan.next ();
	ASSERT_EQ (start.number (), 1);

	// Create response with more candidates than limit
	// Response contains: 1, 4, 7, 10
	std::deque<std::pair<nano::account, nano::block_hash>> response1;
	for (int i = 0; i <= 9; i += 3)
	{
		response1.push_back ({ nano::account{ start.number () + i }, nano::block_hash{ static_cast<uint64_t> (i) } });
	}
	ASSERT_FALSE (frontier_scan.process (start, response1));

	// Response contains: 1, 3, 5, 7, 9
	std::deque<std::pair<nano::account, nano::block_hash>> response2;
	for (int i = 0; i <= 8; i += 2)
	{
		response2.push_back ({ nano::account{ start.number () + i }, nano::block_hash{ static_cast<uint64_t> (i) } });
	}
	ASSERT_TRUE (frontier_scan.process (start, response2));

	// After processing replies candidates should be ordered and trimmed
	auto next = frontier_scan.next ();
	ASSERT_EQ (next.number (), 5);
}

TEST (bootstrap_frontier_scan, heads_distribution)
{
	nano::frontier_scan_config config;
	config.head_parallelism = 4;
	test_context ctx{ config };
	auto & frontier_scan = ctx.frontier_scan;

	// Collect initial accounts from each head
	std::vector<nano::account> initial_accounts;
	for (int i = 0; i < 4; i++)
	{
		initial_accounts.push_back (frontier_scan.next ());
	}

	// Verify accounts are properly distributed across the range
	for (size_t i = 1; i < initial_accounts.size (); i++)
	{
		ASSERT_GT (initial_accounts[i].number (), initial_accounts[i - 1].number ());
	}
}

TEST (bootstrap_frontier_scan, invalid_response_ordering)
{
	nano::frontier_scan_config config;
	config.head_parallelism = 1;
	config.consideration_count = 1;
	test_context ctx{ config };
	auto & frontier_scan = ctx.frontier_scan;

	auto start = frontier_scan.next ();

	// Create response with out-of-order accounts
	std::deque<std::pair<nano::account, nano::block_hash>> response;
	response.push_back ({ nano::account{ start.number () + 2 }, nano::block_hash{ 1 } });
	response.push_back ({ nano::account{ start.number () + 1 }, nano::block_hash{ 2 } }); // Out of order

	// Should still process successfully
	ASSERT_TRUE (frontier_scan.process (start, response));
	ASSERT_EQ (frontier_scan.next (), start.number () + 2);
}

TEST (bootstrap_frontier_scan, empty_responses)
{
	nano::frontier_scan_config config;
	config.head_parallelism = 1;
	config.consideration_count = 2;
	test_context ctx{ config };
	auto & frontier_scan = ctx.frontier_scan;

	auto start = frontier_scan.next ();

	// Empty response should not advance head even after receiving `consideration_count` responses
	std::deque<std::pair<nano::account, nano::block_hash>> empty_response;
	ASSERT_FALSE (frontier_scan.process (start, empty_response));
	ASSERT_FALSE (frontier_scan.process (start, empty_response));
	ASSERT_EQ (frontier_scan.next (), start);

	// Let the head advance
	std::deque<std::pair<nano::account, nano::block_hash>> response;
	response.push_back ({ nano::account{ start.number () + 1 }, nano::block_hash{ 1 } });
	ASSERT_TRUE (frontier_scan.process (start, response));
	ASSERT_EQ (frontier_scan.next (), start.number () + 1);

	// However, after receiving enough empty responses, head should wrap around to the start
	ASSERT_FALSE (frontier_scan.process (start, empty_response));
	ASSERT_FALSE (frontier_scan.process (start, empty_response));
	ASSERT_FALSE (frontier_scan.process (start, empty_response));
	ASSERT_EQ (frontier_scan.next (), start.number () + 1);
	ASSERT_TRUE (frontier_scan.process (start, empty_response));
	ASSERT_EQ (frontier_scan.next (), start); // Wraps around
}