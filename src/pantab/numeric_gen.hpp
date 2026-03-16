#pragma once

#include <array>
#include <cstddef>
#include <utility>

// The Tableau Hyper API requires Numeric to be templated at compile time
// but the values are only known at runtime. This dispatches a runtime index
// n ∈ [0, N) to a compile-time integral_constant, calling f with it.
//
// Uses a function pointer table for O(1) dispatch. This replaces the previous
// std::variant + std::visit approach which was extremely slow to compile due
// to the cartesian product of two N-element variant visit dispatch tables.
template <std::size_t N, typename F>
constexpr void integral_dispatch(std::size_t n, F &&f) {
  [&]<std::size_t... Is>(std::index_sequence<Is...>) {
    using FnPtr = void (*)(F &);
    const std::array<FnPtr, N> table{
        {static_cast<FnPtr>([](F &fn) {
          fn(std::integral_constant<std::size_t, Is>{});
        })...}};
    table[n](f);
  }(std::make_index_sequence<N>{});
}
