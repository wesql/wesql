﻿/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/rolesanywhere/model/GetProfileRequest.h>
#include <aws/core/utils/json/JsonSerializer.h>

#include <utility>

using namespace Aws::RolesAnywhere::Model;
using namespace Aws::Utils::Json;
using namespace Aws::Utils;

GetProfileRequest::GetProfileRequest() : 
    m_profileIdHasBeenSet(false)
{
}

Aws::String GetProfileRequest::SerializePayload() const
{
  return {};
}



