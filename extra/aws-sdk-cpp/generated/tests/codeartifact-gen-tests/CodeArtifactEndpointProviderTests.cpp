﻿/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <gtest/gtest.h>
#include <aws/testing/AwsTestHelpers.h>

#include <aws/codeartifact/CodeArtifactEndpointProvider.h>


static const char* ALLOCATION_TAG = "AWSCodeArtifactEndpointProviderTests";
using CodeArtifactEndpointProvider = Aws::CodeArtifact::Endpoint::CodeArtifactEndpointProvider;
using EndpointParameters = Aws::Vector<Aws::Endpoint::EndpointParameter>;
using ResolveEndpointOutcome = Aws::Endpoint::ResolveEndpointOutcome;

using EpParam = Aws::Endpoint::EndpointParameter;
using EpProp = Aws::Endpoint::EndpointParameter; // just a container to store test expectations
using ExpEpProps = Aws::UnorderedMap<Aws::String, Aws::Vector<Aws::Vector<EpProp>>>;
using ExpEpAuthScheme = Aws::Vector<EpProp>;
using ExpEpHeaders = Aws::UnorderedMap<Aws::String, Aws::Vector<Aws::String>>;

class CodeArtifactEndpointProviderTests : public ::testing::TestWithParam<size_t> {};

struct CodeArtifactEndpointProviderEndpointTestCase
{
    using OperationParamsFromTest = EndpointParameters;

    struct Expect
    {
        struct Endpoint
        {
            Aws::String url;
            ExpEpAuthScheme authScheme;
            ExpEpProps properties;
            ExpEpHeaders headers;
        } endpoint;
        Aws::String error;
    };
    struct OperationInput
    {
        Aws::String operationName;
        OperationParamsFromTest operationParams;
        OperationParamsFromTest builtinParams;
        OperationParamsFromTest clientParams;
    };

    Aws::String documentation;
    // Specification tells us it is Client Initialization parameters
    // At the same time, specification tells us to test EndpointProvider not the client itself
    // Hence params here will be set as a client params (just like a dedicated field above).
    Aws::Vector<Aws::Endpoint::EndpointParameter> params;
    Aws::Vector<Aws::String> tags;
    Expect expect;
    // Aws::Vector<OperationInput> operationInput;
};

static const Aws::Vector<CodeArtifactEndpointProviderEndpointTestCase> TEST_CASES = {
  /*TEST CASE 0*/
  {"For region ap-northeast-1 with FIPS disabled and DualStack disabled", // documentation
    {EpParam("UseFIPS", false), EpParam("Region", "ap-northeast-1"), EpParam("UseDualStack", false)}, // params
    {}, // tags
    {{/*epUrl*/"https://codeartifact.ap-northeast-1.amazonaws.com",
       {/*authScheme*/}, 
       {/*properties*/},
       {/*headers*/}}, {/*No error*/}} // expect
  },
  /*TEST CASE 1*/
  {"For region ap-south-1 with FIPS disabled and DualStack disabled", // documentation
    {EpParam("UseFIPS", false), EpParam("Region", "ap-south-1"), EpParam("UseDualStack", false)}, // params
    {}, // tags
    {{/*epUrl*/"https://codeartifact.ap-south-1.amazonaws.com",
       {/*authScheme*/}, 
       {/*properties*/},
       {/*headers*/}}, {/*No error*/}} // expect
  },
  /*TEST CASE 2*/
  {"For region ap-southeast-1 with FIPS disabled and DualStack disabled", // documentation
    {EpParam("UseFIPS", false), EpParam("Region", "ap-southeast-1"), EpParam("UseDualStack", false)}, // params
    {}, // tags
    {{/*epUrl*/"https://codeartifact.ap-southeast-1.amazonaws.com",
       {/*authScheme*/}, 
       {/*properties*/},
       {/*headers*/}}, {/*No error*/}} // expect
  },
  /*TEST CASE 3*/
  {"For region ap-southeast-2 with FIPS disabled and DualStack disabled", // documentation
    {EpParam("UseFIPS", false), EpParam("Region", "ap-southeast-2"), EpParam("UseDualStack", false)}, // params
    {}, // tags
    {{/*epUrl*/"https://codeartifact.ap-southeast-2.amazonaws.com",
       {/*authScheme*/}, 
       {/*properties*/},
       {/*headers*/}}, {/*No error*/}} // expect
  },
  /*TEST CASE 4*/
  {"For region eu-central-1 with FIPS disabled and DualStack disabled", // documentation
    {EpParam("UseFIPS", false), EpParam("Region", "eu-central-1"), EpParam("UseDualStack", false)}, // params
    {}, // tags
    {{/*epUrl*/"https://codeartifact.eu-central-1.amazonaws.com",
       {/*authScheme*/}, 
       {/*properties*/},
       {/*headers*/}}, {/*No error*/}} // expect
  },
  /*TEST CASE 5*/
  {"For region eu-north-1 with FIPS disabled and DualStack disabled", // documentation
    {EpParam("UseFIPS", false), EpParam("Region", "eu-north-1"), EpParam("UseDualStack", false)}, // params
    {}, // tags
    {{/*epUrl*/"https://codeartifact.eu-north-1.amazonaws.com",
       {/*authScheme*/}, 
       {/*properties*/},
       {/*headers*/}}, {/*No error*/}} // expect
  },
  /*TEST CASE 6*/
  {"For region eu-south-1 with FIPS disabled and DualStack disabled", // documentation
    {EpParam("UseFIPS", false), EpParam("Region", "eu-south-1"), EpParam("UseDualStack", false)}, // params
    {}, // tags
    {{/*epUrl*/"https://codeartifact.eu-south-1.amazonaws.com",
       {/*authScheme*/}, 
       {/*properties*/},
       {/*headers*/}}, {/*No error*/}} // expect
  },
  /*TEST CASE 7*/
  {"For region eu-west-1 with FIPS disabled and DualStack disabled", // documentation
    {EpParam("UseFIPS", false), EpParam("Region", "eu-west-1"), EpParam("UseDualStack", false)}, // params
    {}, // tags
    {{/*epUrl*/"https://codeartifact.eu-west-1.amazonaws.com",
       {/*authScheme*/}, 
       {/*properties*/},
       {/*headers*/}}, {/*No error*/}} // expect
  },
  /*TEST CASE 8*/
  {"For region eu-west-2 with FIPS disabled and DualStack disabled", // documentation
    {EpParam("UseFIPS", false), EpParam("Region", "eu-west-2"), EpParam("UseDualStack", false)}, // params
    {}, // tags
    {{/*epUrl*/"https://codeartifact.eu-west-2.amazonaws.com",
       {/*authScheme*/}, 
       {/*properties*/},
       {/*headers*/}}, {/*No error*/}} // expect
  },
  /*TEST CASE 9*/
  {"For region eu-west-3 with FIPS disabled and DualStack disabled", // documentation
    {EpParam("UseFIPS", false), EpParam("Region", "eu-west-3"), EpParam("UseDualStack", false)}, // params
    {}, // tags
    {{/*epUrl*/"https://codeartifact.eu-west-3.amazonaws.com",
       {/*authScheme*/}, 
       {/*properties*/},
       {/*headers*/}}, {/*No error*/}} // expect
  },
  /*TEST CASE 10*/
  {"For region us-east-1 with FIPS disabled and DualStack disabled", // documentation
    {EpParam("UseFIPS", false), EpParam("Region", "us-east-1"), EpParam("UseDualStack", false)}, // params
    {}, // tags
    {{/*epUrl*/"https://codeartifact.us-east-1.amazonaws.com",
       {/*authScheme*/}, 
       {/*properties*/},
       {/*headers*/}}, {/*No error*/}} // expect
  },
  /*TEST CASE 11*/
  {"For region us-east-2 with FIPS disabled and DualStack disabled", // documentation
    {EpParam("UseFIPS", false), EpParam("Region", "us-east-2"), EpParam("UseDualStack", false)}, // params
    {}, // tags
    {{/*epUrl*/"https://codeartifact.us-east-2.amazonaws.com",
       {/*authScheme*/}, 
       {/*properties*/},
       {/*headers*/}}, {/*No error*/}} // expect
  },
  /*TEST CASE 12*/
  {"For region us-west-2 with FIPS disabled and DualStack disabled", // documentation
    {EpParam("UseFIPS", false), EpParam("Region", "us-west-2"), EpParam("UseDualStack", false)}, // params
    {}, // tags
    {{/*epUrl*/"https://codeartifact.us-west-2.amazonaws.com",
       {/*authScheme*/}, 
       {/*properties*/},
       {/*headers*/}}, {/*No error*/}} // expect
  },
  /*TEST CASE 13*/
  {"For region us-east-1 with FIPS enabled and DualStack enabled", // documentation
    {EpParam("UseFIPS", true), EpParam("Region", "us-east-1"), EpParam("UseDualStack", true)}, // params
    {}, // tags
    {{/*epUrl*/"https://codeartifact-fips.us-east-1.api.aws",
       {/*authScheme*/}, 
       {/*properties*/},
       {/*headers*/}}, {/*No error*/}} // expect
  },
  /*TEST CASE 14*/
  {"For region us-east-1 with FIPS enabled and DualStack disabled", // documentation
    {EpParam("UseFIPS", true), EpParam("Region", "us-east-1"), EpParam("UseDualStack", false)}, // params
    {}, // tags
    {{/*epUrl*/"https://codeartifact-fips.us-east-1.amazonaws.com",
       {/*authScheme*/}, 
       {/*properties*/},
       {/*headers*/}}, {/*No error*/}} // expect
  },
  /*TEST CASE 15*/
  {"For region us-east-1 with FIPS disabled and DualStack enabled", // documentation
    {EpParam("UseFIPS", false), EpParam("Region", "us-east-1"), EpParam("UseDualStack", true)}, // params
    {}, // tags
    {{/*epUrl*/"https://codeartifact.us-east-1.api.aws",
       {/*authScheme*/}, 
       {/*properties*/},
       {/*headers*/}}, {/*No error*/}} // expect
  },
  /*TEST CASE 16*/
  {"For region cn-north-1 with FIPS enabled and DualStack enabled", // documentation
    {EpParam("UseFIPS", true), EpParam("Region", "cn-north-1"), EpParam("UseDualStack", true)}, // params
    {}, // tags
    {{/*epUrl*/"https://codeartifact-fips.cn-north-1.api.amazonwebservices.com.cn",
       {/*authScheme*/}, 
       {/*properties*/},
       {/*headers*/}}, {/*No error*/}} // expect
  },
  /*TEST CASE 17*/
  {"For region cn-north-1 with FIPS enabled and DualStack disabled", // documentation
    {EpParam("UseFIPS", true), EpParam("Region", "cn-north-1"), EpParam("UseDualStack", false)}, // params
    {}, // tags
    {{/*epUrl*/"https://codeartifact-fips.cn-north-1.amazonaws.com.cn",
       {/*authScheme*/}, 
       {/*properties*/},
       {/*headers*/}}, {/*No error*/}} // expect
  },
  /*TEST CASE 18*/
  {"For region cn-north-1 with FIPS disabled and DualStack enabled", // documentation
    {EpParam("UseFIPS", false), EpParam("Region", "cn-north-1"), EpParam("UseDualStack", true)}, // params
    {}, // tags
    {{/*epUrl*/"https://codeartifact.cn-north-1.api.amazonwebservices.com.cn",
       {/*authScheme*/}, 
       {/*properties*/},
       {/*headers*/}}, {/*No error*/}} // expect
  },
  /*TEST CASE 19*/
  {"For region cn-north-1 with FIPS disabled and DualStack disabled", // documentation
    {EpParam("UseFIPS", false), EpParam("Region", "cn-north-1"), EpParam("UseDualStack", false)}, // params
    {}, // tags
    {{/*epUrl*/"https://codeartifact.cn-north-1.amazonaws.com.cn",
       {/*authScheme*/}, 
       {/*properties*/},
       {/*headers*/}}, {/*No error*/}} // expect
  },
  /*TEST CASE 20*/
  {"For region us-gov-east-1 with FIPS enabled and DualStack enabled", // documentation
    {EpParam("UseFIPS", true), EpParam("Region", "us-gov-east-1"), EpParam("UseDualStack", true)}, // params
    {}, // tags
    {{/*epUrl*/"https://codeartifact-fips.us-gov-east-1.api.aws",
       {/*authScheme*/}, 
       {/*properties*/},
       {/*headers*/}}, {/*No error*/}} // expect
  },
  /*TEST CASE 21*/
  {"For region us-gov-east-1 with FIPS enabled and DualStack disabled", // documentation
    {EpParam("UseFIPS", true), EpParam("Region", "us-gov-east-1"), EpParam("UseDualStack", false)}, // params
    {}, // tags
    {{/*epUrl*/"https://codeartifact-fips.us-gov-east-1.amazonaws.com",
       {/*authScheme*/}, 
       {/*properties*/},
       {/*headers*/}}, {/*No error*/}} // expect
  },
  /*TEST CASE 22*/
  {"For region us-gov-east-1 with FIPS disabled and DualStack enabled", // documentation
    {EpParam("UseFIPS", false), EpParam("Region", "us-gov-east-1"), EpParam("UseDualStack", true)}, // params
    {}, // tags
    {{/*epUrl*/"https://codeartifact.us-gov-east-1.api.aws",
       {/*authScheme*/}, 
       {/*properties*/},
       {/*headers*/}}, {/*No error*/}} // expect
  },
  /*TEST CASE 23*/
  {"For region us-gov-east-1 with FIPS disabled and DualStack disabled", // documentation
    {EpParam("UseFIPS", false), EpParam("Region", "us-gov-east-1"), EpParam("UseDualStack", false)}, // params
    {}, // tags
    {{/*epUrl*/"https://codeartifact.us-gov-east-1.amazonaws.com",
       {/*authScheme*/}, 
       {/*properties*/},
       {/*headers*/}}, {/*No error*/}} // expect
  },
  /*TEST CASE 24*/
  {"For region us-iso-east-1 with FIPS enabled and DualStack enabled", // documentation
    {EpParam("UseFIPS", true), EpParam("Region", "us-iso-east-1"), EpParam("UseDualStack", true)}, // params
    {}, // tags
    {{/*No endpoint expected*/}, /*error*/"FIPS and DualStack are enabled, but this partition does not support one or both"} // expect
  },
  /*TEST CASE 25*/
  {"For region us-iso-east-1 with FIPS enabled and DualStack disabled", // documentation
    {EpParam("UseFIPS", true), EpParam("Region", "us-iso-east-1"), EpParam("UseDualStack", false)}, // params
    {}, // tags
    {{/*epUrl*/"https://codeartifact-fips.us-iso-east-1.c2s.ic.gov",
       {/*authScheme*/}, 
       {/*properties*/},
       {/*headers*/}}, {/*No error*/}} // expect
  },
  /*TEST CASE 26*/
  {"For region us-iso-east-1 with FIPS disabled and DualStack enabled", // documentation
    {EpParam("UseFIPS", false), EpParam("Region", "us-iso-east-1"), EpParam("UseDualStack", true)}, // params
    {}, // tags
    {{/*No endpoint expected*/}, /*error*/"DualStack is enabled but this partition does not support DualStack"} // expect
  },
  /*TEST CASE 27*/
  {"For region us-iso-east-1 with FIPS disabled and DualStack disabled", // documentation
    {EpParam("UseFIPS", false), EpParam("Region", "us-iso-east-1"), EpParam("UseDualStack", false)}, // params
    {}, // tags
    {{/*epUrl*/"https://codeartifact.us-iso-east-1.c2s.ic.gov",
       {/*authScheme*/}, 
       {/*properties*/},
       {/*headers*/}}, {/*No error*/}} // expect
  },
  /*TEST CASE 28*/
  {"For region us-isob-east-1 with FIPS enabled and DualStack enabled", // documentation
    {EpParam("UseFIPS", true), EpParam("Region", "us-isob-east-1"), EpParam("UseDualStack", true)}, // params
    {}, // tags
    {{/*No endpoint expected*/}, /*error*/"FIPS and DualStack are enabled, but this partition does not support one or both"} // expect
  },
  /*TEST CASE 29*/
  {"For region us-isob-east-1 with FIPS enabled and DualStack disabled", // documentation
    {EpParam("UseFIPS", true), EpParam("Region", "us-isob-east-1"), EpParam("UseDualStack", false)}, // params
    {}, // tags
    {{/*epUrl*/"https://codeartifact-fips.us-isob-east-1.sc2s.sgov.gov",
       {/*authScheme*/}, 
       {/*properties*/},
       {/*headers*/}}, {/*No error*/}} // expect
  },
  /*TEST CASE 30*/
  {"For region us-isob-east-1 with FIPS disabled and DualStack enabled", // documentation
    {EpParam("UseFIPS", false), EpParam("Region", "us-isob-east-1"), EpParam("UseDualStack", true)}, // params
    {}, // tags
    {{/*No endpoint expected*/}, /*error*/"DualStack is enabled but this partition does not support DualStack"} // expect
  },
  /*TEST CASE 31*/
  {"For region us-isob-east-1 with FIPS disabled and DualStack disabled", // documentation
    {EpParam("UseFIPS", false), EpParam("Region", "us-isob-east-1"), EpParam("UseDualStack", false)}, // params
    {}, // tags
    {{/*epUrl*/"https://codeartifact.us-isob-east-1.sc2s.sgov.gov",
       {/*authScheme*/}, 
       {/*properties*/},
       {/*headers*/}}, {/*No error*/}} // expect
  },
  /*TEST CASE 32*/
  {"For custom endpoint with region set and fips disabled and dualstack disabled", // documentation
    {EpParam("UseFIPS", false), EpParam("Endpoint", "https://example.com"), EpParam("Region", "us-east-1"), EpParam("UseDualStack", false)}, // params
    {}, // tags
    {{/*epUrl*/"https://example.com",
       {/*authScheme*/}, 
       {/*properties*/},
       {/*headers*/}}, {/*No error*/}} // expect
  },
  /*TEST CASE 33*/
  {"For custom endpoint with region not set and fips disabled and dualstack disabled", // documentation
    {EpParam("UseFIPS", false), EpParam("Endpoint", "https://example.com"), EpParam("UseDualStack", false)}, // params
    {}, // tags
    {{/*epUrl*/"https://example.com",
       {/*authScheme*/}, 
       {/*properties*/},
       {/*headers*/}}, {/*No error*/}} // expect
  },
  /*TEST CASE 34*/
  {"For custom endpoint with fips enabled and dualstack disabled", // documentation
    {EpParam("UseFIPS", true), EpParam("Endpoint", "https://example.com"), EpParam("Region", "us-east-1"), EpParam("UseDualStack", false)}, // params
    {}, // tags
    {{/*No endpoint expected*/}, /*error*/"Invalid Configuration: FIPS and custom endpoint are not supported"} // expect
  },
  /*TEST CASE 35*/
  {"For custom endpoint with fips disabled and dualstack enabled", // documentation
    {EpParam("UseFIPS", false), EpParam("Endpoint", "https://example.com"), EpParam("Region", "us-east-1"), EpParam("UseDualStack", true)}, // params
    {}, // tags
    {{/*No endpoint expected*/}, /*error*/"Invalid Configuration: Dualstack and custom endpoint are not supported"} // expect
  },
  /*TEST CASE 36*/
  {"Missing region", // documentation
    {}, // params
    {}, // tags
    {{/*No endpoint expected*/}, /*error*/"Invalid Configuration: Missing Region"} // expect
  }
};

Aws::String RulesToSdkSignerName(const Aws::String& rulesSignerName)
{
    Aws::String sdkSigner = "NullSigner";
    if (rulesSignerName == "sigv4") {
        sdkSigner = "SignatureV4";
    } else if (rulesSignerName == "sigv4a") {
        sdkSigner = "AsymmetricSignatureV4";
    } else if (rulesSignerName == "none") {
        sdkSigner = "NullSigner";
    } else if (rulesSignerName == "bearer") {
        sdkSigner = "Bearer";
    } else if (rulesSignerName == "s3Express") {
        sdkSigner = "S3ExpressSigner";
    } else {
        sdkSigner = rulesSignerName;
    }

    return sdkSigner;
}

void ValidateOutcome(const ResolveEndpointOutcome& outcome, const CodeArtifactEndpointProviderEndpointTestCase::Expect& expect)
{
    if(!expect.error.empty())
    {
        ASSERT_FALSE(outcome.IsSuccess()) << "Expected failure with message:\n" << expect.error;
        ASSERT_EQ(outcome.GetError().GetMessage(), expect.error);
    }
    else
    {
        AWS_ASSERT_SUCCESS(outcome);
        ASSERT_EQ(outcome.GetResult().GetURL(), expect.endpoint.url);
        const auto expAuthSchemesIt = expect.endpoint.properties.find("authSchemes");
        if (expAuthSchemesIt != expect.endpoint.properties.end())
        {
            // in the list of AuthSchemes, select the one with a highest priority
            const Aws::Vector<Aws::String> priotityList = {"s3Express", "sigv4a", "sigv4", "bearer", "none", ""};
            const auto expectedAuthSchemePropsIt = std::find_first_of(expAuthSchemesIt->second.begin(), expAuthSchemesIt->second.end(),
                                                                    priotityList.begin(), priotityList.end(), [](const Aws::Vector<EpProp>& props, const Aws::String& expName)
                                                                    {
                                                                        const auto& propNameIt = std::find_if(props.begin(), props.end(), [](const EpProp& prop)
                                                                        {
                                                                            return prop.GetName() == "name";
                                                                        });
                                                                        assert(propNameIt != props.end());
                                                                        return propNameIt->GetStrValueNoCheck() == expName;
                                                                    });
            assert(expectedAuthSchemePropsIt != expAuthSchemesIt->second.end());

            const auto& endpointResultAttrs = outcome.GetResult().GetAttributes();
            ASSERT_TRUE(endpointResultAttrs) << "Expected non-empty EndpointAttributes (authSchemes)";
            for (const auto& expProperty : *expectedAuthSchemePropsIt)
            {
                if (expProperty.GetName() == "name") {
                    ASSERT_TRUE(!endpointResultAttrs->authScheme.GetName().empty());
                    ASSERT_EQ(RulesToSdkSignerName(expProperty.GetStrValueNoCheck()), endpointResultAttrs->authScheme.GetName());
                } else if (expProperty.GetName() == "signingName") {
                    ASSERT_TRUE(endpointResultAttrs->authScheme.GetSigningName());
                    ASSERT_EQ(expProperty.GetStrValueNoCheck(), endpointResultAttrs->authScheme.GetSigningName().value());
                } else if (expProperty.GetName() == "signingRegion") {
                    ASSERT_TRUE(endpointResultAttrs->authScheme.GetSigningRegion());
                    ASSERT_EQ(expProperty.GetStrValueNoCheck(), endpointResultAttrs->authScheme.GetSigningRegion().value());
                } else if (expProperty.GetName() == "signingRegionSet") {
                    ASSERT_TRUE(endpointResultAttrs->authScheme.GetSigningRegionSet());
                    ASSERT_EQ(expProperty.GetStrValueNoCheck(), endpointResultAttrs->authScheme.GetSigningRegionSet().value());
                } else if (expProperty.GetName() == "disableDoubleEncoding") {
                    ASSERT_TRUE(endpointResultAttrs->authScheme.GetDisableDoubleEncoding());
                    ASSERT_EQ(expProperty.GetBoolValueNoCheck(), endpointResultAttrs->authScheme.GetDisableDoubleEncoding().value());
                } else {
                    FAIL() << "Unsupported Auth type property " << expProperty.GetName() << ". Need to update test.";
                }
            }
        }

        EXPECT_EQ(expect.endpoint.headers.empty(), outcome.GetResult().GetHeaders().empty());
        for(const auto& expHeaderVec : expect.endpoint.headers)
        {
            const auto& retHeaderIt = outcome.GetResult().GetHeaders().find(expHeaderVec.first);
            ASSERT_TRUE(retHeaderIt != outcome.GetResult().GetHeaders().end());

            auto retHeaderVec = Aws::Utils::StringUtils::Split(retHeaderIt->second, ';');
            std::sort(retHeaderVec.begin(), retHeaderVec.end());

            auto expHeaderVecSorted = expHeaderVec.second;
            std::sort(expHeaderVecSorted.begin(), expHeaderVecSorted.end());

            ASSERT_EQ(expHeaderVecSorted, retHeaderVec);
        }
    }
}

TEST_P(CodeArtifactEndpointProviderTests, EndpointProviderTest)
{
    const size_t TEST_CASE_IDX = GetParam();
    ASSERT_LT(TEST_CASE_IDX, TEST_CASES.size()) << "Something is wrong with the test fixture itself.";
    const CodeArtifactEndpointProviderEndpointTestCase& TEST_CASE = TEST_CASES.at(TEST_CASE_IDX);
    SCOPED_TRACE(Aws::String("\nTEST CASE # ") + Aws::Utils::StringUtils::to_string(TEST_CASE_IDX) + ": " + TEST_CASE.documentation);

    std::shared_ptr<CodeArtifactEndpointProvider> endpointProvider = Aws::MakeShared<CodeArtifactEndpointProvider>(ALLOCATION_TAG);
    ASSERT_TRUE(endpointProvider) << "Failed to allocate/initialize CodeArtifactEndpointProvider";

    EndpointParameters endpointParameters;
    for(const auto& param : TEST_CASE.params)
    {
        endpointParameters.emplace(endpointParameters.end(), Aws::Endpoint::EndpointParameter(param));
    }
    auto resolvedEndpointOutcome = endpointProvider->ResolveEndpoint(endpointParameters);
    ValidateOutcome(resolvedEndpointOutcome, TEST_CASE.expect);

#if 0 // temporarily disabled
    for(const auto& operation : TEST_CASE.operationInput)
    {
        /*
         * Most specific to least specific value locations:
            staticContextParams
            contextParam
            clientContextParams
            Built-In Bindings
            Built-in binding default values
         */
        const Aws::Vector<std::reference_wrapper<const CodeArtifactEndpointProviderEndpointTestCase::OperationParamsFromTest>>
                operationInputParams = {std::cref(operation.builtinParams), std::cref(operation.clientParams), std::cref(operation.operationParams)};

        for(const auto& paramSource : operationInputParams)
        {
            for(const auto& param : paramSource.get())
            {
                endpointParameters.emplace(endpointParameters.end(), Aws::Endpoint::EndpointParameter(param));
            }
        }
        auto resolvedEndpointOutcomePerOperation = endpointProvider->ResolveEndpoint(endpointParameters);
        ValidateOutcome(resolvedEndpointOutcomePerOperation, TEST_CASE.expect);
    }
#endif
}

INSTANTIATE_TEST_SUITE_P(EndpointTestsFromModel,
                         CodeArtifactEndpointProviderTests,
                         ::testing::Range((size_t) 0u, TEST_CASES.size()));