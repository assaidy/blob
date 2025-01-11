package blob

import "net/http"

type APIError struct {
	Code          int    `json:"-"`
	Message       string `json:"message"`
	Errors        any    `json:"errors,omitempty"`
	InternalError error  `json:"-"`
}

func (e *APIError) Error() string {
	if e.InternalError == nil {
		return e.Message
	}
	return e.InternalError.Error()
}

func InvalidJsonRequestError() *APIError {
	return &APIError{
		Code:    http.StatusBadRequest,
		Message: "failed to parse request body",
	}
}

func BadRequestError(msg string) *APIError {
	return &APIError{
		Code:    http.StatusBadRequest,
		Message: msg,
	}
}

func InvalidDataError(msg string) *APIError {
	return &APIError{
		Code:    http.StatusUnprocessableEntity,
		Message: msg,
	}
}

func ValidationError(errs any) *APIError {
	return &APIError{
		Code:    http.StatusUnprocessableEntity,
		Message: "invalid request data",
		Errors:  errs,
	}
}

func ConflictError(msg string) *APIError {
	return &APIError{
		Code:    http.StatusConflict,
		Message: msg,
	}
}

func NotFoundError(msg string) *APIError {
	return &APIError{
		Code:    http.StatusNotFound,
		Message: msg,
	}
}

func InternalServerError(err error) *APIError {
	return &APIError{
		Code:          http.StatusInternalServerError,
		Message:       "internal server error",
		InternalError: err,
	}
}

func UnauthorizedError() *APIError {
	return &APIError{
		Code:    http.StatusUnauthorized,
		Message: "unauthorized",
	}
}
