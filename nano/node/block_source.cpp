#include <nano/lib/enum_util.hpp>
#include <nano/lib/stats_enums.hpp>
#include <nano/node/block_source.hpp>

std::string_view nano::to_string (nano::block_source source)
{
	return nano::enum_util::name (source);
}

nano::stat::detail nano::to_stat_detail (nano::block_source type)
{
	return nano::enum_util::cast<nano::stat::detail> (type);
}