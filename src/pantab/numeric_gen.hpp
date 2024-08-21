#pragma once

#include <cstddef>
#include <utility>
#include <variant>

// The Tableau Hyper API requires Numeric to be templated at compile time
// but the values are only known at runtime. This solution is adopted from
// https://stackoverflow.com/questions/78888913/creating-cartesian-product-from-integer-range-template-argument/78889229?noredirect=1#comment139097273_78889229
template <std::size_t N> constexpr auto to_integral_variant(std::size_t n) {
  return [&]<std::size_t... Is>(std::index_sequence<Is...>) {
    using ResType = std::variant<std::integral_constant<std::size_t, Is>...>;
    ResType all[] = {ResType{std::integral_constant<std::size_t, Is>{}}...};
    return all[n];
  }(std::make_index_sequence<N>());
}
