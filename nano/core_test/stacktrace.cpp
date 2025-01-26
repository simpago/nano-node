#include <nano/lib/stacktrace.hpp>

#include <gtest/gtest.h>

// Check that the stacktrace contains the current function name
// This depends on the way testcase names are compiled by gtest
// Current name: "stacktrace_human_readable_Test::TestBody()"
TEST (stacktrace, human_readable)
{
	auto stacktrace = nano::generate_stacktrace ();
	std::cout << stacktrace << std::endl;
	ASSERT_FALSE (stacktrace.empty ());
	ASSERT_TRUE (stacktrace.find ("stacktrace_human_readable_Test") != std::string::npos);
}