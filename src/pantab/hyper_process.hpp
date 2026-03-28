#pragma once

#include <string>
#include <unordered_map>

#include <hyperapi/hyperapi.hpp>

static inline auto
MakeHyperProcess(std::unordered_map<std::string, std::string> process_params)
    -> hyperapi::HyperProcess {
  process_params.emplace("log_config", "");
  process_params.emplace("default_database_version", "2");
  return hyperapi::HyperProcess{
      hyperapi::Telemetry::DoNotSendUsageDataToTableau, "",
      std::move(process_params)};
}
