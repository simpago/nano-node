#pragma once

#include <functional>
#include <memory>
#include <utility>

namespace nano
{
// TODO: Replace with std::move_only_function in C++23
template <typename F>
auto wrap_move_only (F && f)
{
	using fn_type = decltype (std::function{ std::declval<F> () });
	auto ptr = std::make_shared<std::decay_t<F>> (std::forward<F> (f));
	return fn_type ([ptr] (auto &&... args) {
		return (*ptr) (std::forward<decltype (args)> (args)...);
	});
}
}