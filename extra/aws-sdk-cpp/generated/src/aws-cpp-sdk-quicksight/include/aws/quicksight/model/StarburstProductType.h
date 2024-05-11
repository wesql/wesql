﻿/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/quicksight/QuickSight_EXPORTS.h>
#include <aws/core/utils/memory/stl/AWSString.h>

namespace Aws
{
namespace QuickSight
{
namespace Model
{
  enum class StarburstProductType
  {
    NOT_SET,
    GALAXY,
    ENTERPRISE
  };

namespace StarburstProductTypeMapper
{
AWS_QUICKSIGHT_API StarburstProductType GetStarburstProductTypeForName(const Aws::String& name);

AWS_QUICKSIGHT_API Aws::String GetNameForStarburstProductType(StarburstProductType value);
} // namespace StarburstProductTypeMapper
} // namespace Model
} // namespace QuickSight
} // namespace Aws