﻿/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once
#include <aws/datasync/DataSync_EXPORTS.h>
#include <aws/datasync/DataSyncRequest.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <utility>

namespace Aws
{
namespace DataSync
{
namespace Model
{

  /**
   * <p>DescribeLocationNfsRequest</p><p><h3>See Also:</h3>   <a
   * href="http://docs.aws.amazon.com/goto/WebAPI/datasync-2018-11-09/DescribeLocationNfsRequest">AWS
   * API Reference</a></p>
   */
  class DescribeLocationNfsRequest : public DataSyncRequest
  {
  public:
    AWS_DATASYNC_API DescribeLocationNfsRequest();

    // Service request name is the Operation name which will send this request out,
    // each operation should has unique request name, so that we can get operation's name from this request.
    // Note: this is not true for response, multiple operations may have the same response name,
    // so we can not get operation's name from response.
    inline virtual const char* GetServiceRequestName() const override { return "DescribeLocationNfs"; }

    AWS_DATASYNC_API Aws::String SerializePayload() const override;

    AWS_DATASYNC_API Aws::Http::HeaderValueCollection GetRequestSpecificHeaders() const override;


    /**
     * <p>Specifies the Amazon Resource Name (ARN) of the NFS location that you want
     * information about.</p>
     */
    inline const Aws::String& GetLocationArn() const{ return m_locationArn; }

    /**
     * <p>Specifies the Amazon Resource Name (ARN) of the NFS location that you want
     * information about.</p>
     */
    inline bool LocationArnHasBeenSet() const { return m_locationArnHasBeenSet; }

    /**
     * <p>Specifies the Amazon Resource Name (ARN) of the NFS location that you want
     * information about.</p>
     */
    inline void SetLocationArn(const Aws::String& value) { m_locationArnHasBeenSet = true; m_locationArn = value; }

    /**
     * <p>Specifies the Amazon Resource Name (ARN) of the NFS location that you want
     * information about.</p>
     */
    inline void SetLocationArn(Aws::String&& value) { m_locationArnHasBeenSet = true; m_locationArn = std::move(value); }

    /**
     * <p>Specifies the Amazon Resource Name (ARN) of the NFS location that you want
     * information about.</p>
     */
    inline void SetLocationArn(const char* value) { m_locationArnHasBeenSet = true; m_locationArn.assign(value); }

    /**
     * <p>Specifies the Amazon Resource Name (ARN) of the NFS location that you want
     * information about.</p>
     */
    inline DescribeLocationNfsRequest& WithLocationArn(const Aws::String& value) { SetLocationArn(value); return *this;}

    /**
     * <p>Specifies the Amazon Resource Name (ARN) of the NFS location that you want
     * information about.</p>
     */
    inline DescribeLocationNfsRequest& WithLocationArn(Aws::String&& value) { SetLocationArn(std::move(value)); return *this;}

    /**
     * <p>Specifies the Amazon Resource Name (ARN) of the NFS location that you want
     * information about.</p>
     */
    inline DescribeLocationNfsRequest& WithLocationArn(const char* value) { SetLocationArn(value); return *this;}

  private:

    Aws::String m_locationArn;
    bool m_locationArnHasBeenSet = false;
  };

} // namespace Model
} // namespace DataSync
} // namespace Aws