/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "velox/connectors/hive/HiveConfig.h"
#include "velox/core/Config.h"
#include "velox/core/QueryConfig.h"

#include <boost/algorithm/string.hpp>

namespace facebook::velox::connector::hive {

namespace {

HiveConfig::InsertExistingPartitionsBehavior
stringToInsertExistingPartitionsBehavior(const std::string& strValue) {
  auto upperValue = boost::algorithm::to_upper_copy(strValue);
  if (upperValue == "ERROR") {
    return HiveConfig::InsertExistingPartitionsBehavior::kError;
  }
  if (upperValue == "OVERWRITE") {
    return HiveConfig::InsertExistingPartitionsBehavior::kOverwrite;
  }
  VELOX_UNSUPPORTED(
      "Unsupported insert existing partitions behavior: {}.", strValue);
}

} // namespace

// static
std::string HiveConfig::insertExistingPartitionsBehaviorString(
    InsertExistingPartitionsBehavior behavior) {
  switch (behavior) {
    case InsertExistingPartitionsBehavior::kError:
      return "ERROR";
    case InsertExistingPartitionsBehavior::kOverwrite:
      return "OVERWRITE";
    default:
      return fmt::format("UNKNOWN BEHAVIOR {}", static_cast<int>(behavior));
  }
}

HiveConfig::InsertExistingPartitionsBehavior
HiveConfig::insertExistingPartitionsBehavior(const Config* session) const {
  if (session->isValueExists(kInsertExistingPartitionsBehaviorSession)) {
    return stringToInsertExistingPartitionsBehavior(
        session->get<std::string>(kInsertExistingPartitionsBehaviorSession)
            .value());
  }
  const auto behavior =
      config_->get<std::string>(kInsertExistingPartitionsBehavior);
  return behavior.has_value()
      ? stringToInsertExistingPartitionsBehavior(behavior.value())
      : InsertExistingPartitionsBehavior::kError;
}

uint32_t HiveConfig::maxPartitionsPerWriters(const Config* session) const {
  if (session->isValueExists(kMaxPartitionsPerWritersSession)) {
    return session->get<uint32_t>(kMaxPartitionsPerWritersSession).value();
  }
  return config_->get<uint32_t>(kMaxPartitionsPerWriters, 100);
}

bool HiveConfig::immutablePartitions() const {
  return config_->get<bool>(kImmutablePartitions, false);
}

bool HiveConfig::s3UseVirtualAddressing() const {
  return !config_->get(kS3PathStyleAccess, false);
}

std::string HiveConfig::s3GetLogLevel() const {
  return config_->get(kS3LogLevel, std::string("FATAL"));
}

bool HiveConfig::s3UseSSL() const {
  return config_->get(kS3SSLEnabled, true);
}

bool HiveConfig::s3UseInstanceCredentials() const {
  return config_->get(kS3UseInstanceCredentials, false);
}

std::string HiveConfig::s3Endpoint() const {
  return config_->get(kS3Endpoint, std::string(""));
}

std::optional<std::string> HiveConfig::s3AccessKey() const {
  if (config_->isValueExists(kS3AwsAccessKey)) {
    return config_->get(kS3AwsAccessKey).value();
  }
  return {};
}

std::optional<std::string> HiveConfig::s3SecretKey() const {
  if (config_->isValueExists(kS3AwsSecretKey)) {
    return config_->get(kS3AwsSecretKey).value();
  }
  return {};
}

std::optional<std::string> HiveConfig::s3IAMRole() const {
  if (config_->isValueExists(kS3IamRole)) {
    return config_->get(kS3IamRole).value();
  }
  return {};
}

std::string HiveConfig::s3IAMRoleSessionName() const {
  return config_->get(kS3IamRoleSessionName, std::string("velox-session"));
}

std::string HiveConfig::gcsEndpoint() const {
  return config_->get<std::string>(kGCSEndpoint, std::string(""));
}

std::string HiveConfig::gcsScheme() const {
  return config_->get<std::string>(kGCSScheme, std::string("https"));
}

std::string HiveConfig::gcsCredentials() const {
  return config_->get<std::string>(kGCSCredentials, std::string(""));
}

bool HiveConfig::isOrcUseColumnNames(const Config* session) const {
  if (session->isValueExists(kOrcUseColumnNamesSession)) {
    return session->get<bool>(kOrcUseColumnNamesSession).value();
  }
  return config_->get<bool>(kOrcUseColumnNames, false);
}

bool HiveConfig::isFileColumnNamesReadAsLowerCase(const Config* session) const {
  if (session->isValueExists(kFileColumnNamesReadAsLowerCaseSession)) {
    return session->get<bool>(kFileColumnNamesReadAsLowerCaseSession).value();
  }
  return config_->get<bool>(kFileColumnNamesReadAsLowerCase, false);
}

bool HiveConfig::isPartitionPathAsLowerCase(const Config* session) const {
  return config_->get<bool>(kPartitionPathAsLowerCaseSession, true);
}

int64_t HiveConfig::maxCoalescedBytes() const {
  return config_->get<int64_t>(kMaxCoalescedBytes, 128 << 20);
}

int32_t HiveConfig::maxCoalescedDistanceBytes() const {
  return config_->get<int32_t>(kMaxCoalescedDistanceBytes, 512 << 10);
}

int32_t HiveConfig::prefetchRowGroups() const {
  return config_->get<int32_t>(kPrefetchRowGroups, 1);
}

int32_t HiveConfig::loadQuantum() const {
  return config_->get<int32_t>(kLoadQuantum, 8 << 20);
}

int32_t HiveConfig::numCacheFileHandles() const {
  return config_->get<int32_t>(kNumCacheFileHandles, 20'000);
}

bool HiveConfig::isFileHandleCacheEnabled() const {
  return config_->get<bool>(kEnableFileHandleCache, true);
}

uint64_t HiveConfig::orcWriterMaxStripeSize(const Config* session) const {
  if (session->isValueExists(kOrcWriterMaxStripeSizeSession)) {
    return toCapacity(
        session->get<std::string>(kOrcWriterMaxStripeSizeSession).value(),
        core::CapacityUnit::BYTE);
  }
  if (config_->isValueExists(kOrcWriterMaxStripeSize)) {
    return toCapacity(
        config_->get<std::string>(kOrcWriterMaxStripeSize).value(),
        core::CapacityUnit::BYTE);
  }
  return 64L * 1024L * 1024L;
}

uint64_t HiveConfig::orcWriterMaxDictionaryMemory(const Config* session) const {
  if (session->isValueExists(kOrcWriterMaxDictionaryMemorySession)) {
    return toCapacity(
        session->get<std::string>(kOrcWriterMaxDictionaryMemorySession).value(),
        core::CapacityUnit::BYTE);
  }
  if (config_->isValueExists(kOrcWriterMaxDictionaryMemory)) {
    return toCapacity(
        config_->get<std::string>(kOrcWriterMaxDictionaryMemory).value(),
        core::CapacityUnit::BYTE);
  }
  return 16L * 1024L * 1024L;
}

std::string HiveConfig::writeFileCreateConfig() const {
  return config_->get<std::string>(kWriteFileCreateConfig, "");
}

uint32_t HiveConfig::sortWriterMaxOutputRows(const Config* session) const {
  if (session->isValueExists(kSortWriterMaxOutputRowsSession)) {
    return session->get<uint32_t>(kSortWriterMaxOutputRowsSession).value();
  }
  return config_->get<int32_t>(kSortWriterMaxOutputRows, 1024);
}

uint64_t HiveConfig::sortWriterMaxOutputBytes(const Config* session) const {
  if (session->isValueExists(kSortWriterMaxOutputBytesSession)) {
    return toCapacity(
        session->get<std::string>(kSortWriterMaxOutputBytesSession).value(),
        core::CapacityUnit::BYTE);
  }
  if (config_->isValueExists(kSortWriterMaxOutputBytes)) {
    return toCapacity(
        config_->get<std::string>(kSortWriterMaxOutputBytes).value(),
        core::CapacityUnit::BYTE);
  }
  return 10UL << 20;
}

uint64_t HiveConfig::footerEstimatedSize() const {
  return config_->get<uint64_t>(kFooterEstimatedSize, 1UL << 20);
}

uint64_t HiveConfig::filePreloadThreshold() const {
  return config_->get<uint64_t>(kFilePreloadThreshold, 8UL << 20);
}

} // namespace facebook::velox::connector::hive
