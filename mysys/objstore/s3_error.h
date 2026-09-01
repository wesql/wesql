/*
   Copyright (c) 2026, ApeCloud Inc Holding Limited.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.
*/

#ifndef MY_OBJSTORE_S3_ERROR_H_INCLUDED
#define MY_OBJSTORE_S3_ERROR_H_INCLUDED

#include "objstore.h"

namespace objstore::s3_detail {

enum class ObjectErrorKind { NO_SUCH_KEY, NO_SUCH_BUCKET, OTHER };

inline bool is_transient_http_status(int http_status) {
  return http_status == -1 || http_status == 408 || http_status == 429 ||
         (http_status >= 500 && http_status <= 599);
}

inline bool is_http_status(int http_status) {
  return http_status >= 100 && http_status <= 599;
}

inline ExactObjectOutcome classify_exact_get_failure(
    int http_status, bool retryable, ObjectErrorKind error_kind) {
  if (http_status == 404) {
    return error_kind == ObjectErrorKind::NO_SUCH_BUCKET
               ? ExactObjectOutcome::PERMANENT_ERROR
               : ExactObjectOutcome::NOT_FOUND_404;
  }
  if (is_transient_http_status(http_status)) {
    return ExactObjectOutcome::TRANSIENT_UNAVAILABLE;
  }
  if (is_http_status(http_status)) return ExactObjectOutcome::PERMANENT_ERROR;
  if (error_kind == ObjectErrorKind::NO_SUCH_BUCKET) {
    return ExactObjectOutcome::PERMANENT_ERROR;
  }
  if (error_kind == ObjectErrorKind::NO_SUCH_KEY) {
    return ExactObjectOutcome::NOT_FOUND_404;
  }
  if (retryable) return ExactObjectOutcome::TRANSIENT_UNAVAILABLE;
  return ExactObjectOutcome::PERMANENT_ERROR;
}

inline ConditionalPutOutcome classify_conditional_put_failure(int http_status,
                                                              bool retryable) {
  if (http_status == 409) return ConditionalPutOutcome::CONFLICT_409;
  if (http_status == 412) {
    return ConditionalPutOutcome::PRECONDITION_FAILED_412;
  }
  if (is_transient_http_status(http_status)) {
    return ConditionalPutOutcome::TRANSPORT_UNKNOWN;
  }
  if (is_http_status(http_status)) {
    return ConditionalPutOutcome::PERMANENT_ERROR;
  }
  if (retryable) return ConditionalPutOutcome::TRANSPORT_UNKNOWN;
  return ConditionalPutOutcome::PERMANENT_ERROR;
}

}  // namespace objstore::s3_detail

#endif  // MY_OBJSTORE_S3_ERROR_H_INCLUDED
