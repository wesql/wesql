﻿/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/sesv2/SESV2_EXPORTS.h>
#include <aws/sesv2/SESV2Request.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/sesv2/model/ScalingMode.h>
#include <utility>

namespace Aws
{
namespace SESV2
{
namespace Model
{

  /**
   * <p>A request to convert a dedicated IP pool to a different scaling
   * mode.</p><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/sesv2-2019-09-27/PutDedicatedIpPoolScalingAttributesRequest">AWS
   * API Reference</a></p>
   */
  class PutDedicatedIpPoolScalingAttributesRequest : public SESV2Request
  {
  public:
    AWS_SESV2_API PutDedicatedIpPoolScalingAttributesRequest();

    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "PutDedicatedIpPoolScalingAttributes"; }

    AWS_SESV2_API Aws::String SerializePayload() const override;


    /**
     * <p>The name of the dedicated IP pool.</p>
     */
    inline const Aws::String& GetPoolName() const{ return m_poolName; }

    /**
     * <p>The name of the dedicated IP pool.</p>
     */
    inline bool PoolNameHasBeenSet() const { return m_poolNameHasBeenSet; }

    /**
     * <p>The name of the dedicated IP pool.</p>
     */
    inline void SetPoolName(const Aws::String& value) { m_poolNameHasBeenSet = true; m_poolName = value; }

    /**
     * <p>The name of the dedicated IP pool.</p>
     */
    inline void SetPoolName(Aws::String&& value) { m_poolNameHasBeenSet = true; m_poolName = std::move(value); }

    /**
     * <p>The name of the dedicated IP pool.</p>
     */
    inline void SetPoolName(const char* value) { m_poolNameHasBeenSet = true; m_poolName.assign(value); }

    /**
     * <p>The name of the dedicated IP pool.</p>
     */
    inline PutDedicatedIpPoolScalingAttributesRequest& WithPoolName(const Aws::String& value) { SetPoolName(value); return *this;}

    /**
     * <p>The name of the dedicated IP pool.</p>
     */
    inline PutDedicatedIpPoolScalingAttributesRequest& WithPoolName(Aws::String&& value) { SetPoolName(std::move(value)); return *this;}

    /**
     * <p>The name of the dedicated IP pool.</p>
     */
    inline PutDedicatedIpPoolScalingAttributesRequest& WithPoolName(const char* value) { SetPoolName(value); return *this;}


    /**
     * <p>The scaling mode to apply to the dedicated IP pool.</p>  <p>Changing
     * the scaling mode from <code>MANAGED</code> to <code>STANDARD</code> is not
     * supported.</p> 
     */
    inline const ScalingMode& GetScalingMode() const{ return m_scalingMode; }

    /**
     * <p>The scaling mode to apply to the dedicated IP pool.</p>  <p>Changing
     * the scaling mode from <code>MANAGED</code> to <code>STANDARD</code> is not
     * supported.</p> 
     */
    inline bool ScalingModeHasBeenSet() const { return m_scalingModeHasBeenSet; }

    /**
     * <p>The scaling mode to apply to the dedicated IP pool.</p>  <p>Changing
     * the scaling mode from <code>MANAGED</code> to <code>STANDARD</code> is not
     * supported.</p> 
     */
    inline void SetScalingMode(const ScalingMode& value) { m_scalingModeHasBeenSet = true; m_scalingMode = value; }

    /**
     * <p>The scaling mode to apply to the dedicated IP pool.</p>  <p>Changing
     * the scaling mode from <code>MANAGED</code> to <code>STANDARD</code> is not
     * supported.</p> 
     */
    inline void SetScalingMode(ScalingMode&& value) { m_scalingModeHasBeenSet = true; m_scalingMode = std::move(value); }

    /**
     * <p>The scaling mode to apply to the dedicated IP pool.</p>  <p>Changing
     * the scaling mode from <code>MANAGED</code> to <code>STANDARD</code> is not
     * supported.</p> 
     */
    inline PutDedicatedIpPoolScalingAttributesRequest& WithScalingMode(const ScalingMode& value) { SetScalingMode(value); return *this;}

    /**
     * <p>The scaling mode to apply to the dedicated IP pool.</p>  <p>Changing
     * the scaling mode from <code>MANAGED</code> to <code>STANDARD</code> is not
     * supported.</p> 
     */
    inline PutDedicatedIpPoolScalingAttributesRequest& WithScalingMode(ScalingMode&& value) { SetScalingMode(std::move(value)); return *this;}

  private:

    Aws::String m_poolName;
    bool m_poolNameHasBeenSet = false;

    ScalingMode m_scalingMode;
    bool m_scalingModeHasBeenSet = false;
  };

} // namespace Model
} // namespace SESV2
} // namespace Aws