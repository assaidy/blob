package blob

import "github.com/gofiber/fiber/v2"

type ApiError struct {
	Code          int    `json:"-"`
	Message       string `json:"message"`
	Errors        any    `json:"errors,omitempty"`
	InternalError error  `json:"-"`
}

func (e ApiError) Error() string {
	if e.InternalError == nil {
		return e.Message
	}
	return e.InternalError.Error()
}

func InvalidJsonRequestError() ApiError {
	return ApiError{
		Code:    fiber.StatusBadRequest,
		Message: "failed to parse request body",
	}
}

func BadRequestError(msg string) ApiError {
	return ApiError{
		Code:    fiber.StatusBadRequest,
		Message: msg,
	}
}

func InvalidDataError(msg string) ApiError {
	return ApiError{
		Code:    fiber.StatusUnprocessableEntity,
		Message: msg,
	}
}

func ValidationError(errs any) ApiError {
	return ApiError{
		Code:    fiber.StatusUnprocessableEntity,
		Message: "invalid request data",
		Errors:  errs,
	}
}

func ConflictError(msg string) ApiError {
	return ApiError{
		Code:    fiber.StatusConflict,
		Message: msg,
	}
}

func NotFoundError(msg string) ApiError {
	return ApiError{
		Code:    fiber.StatusNotFound,
		Message: msg,
	}
}

func InternalServerError(err error) ApiError {
	return ApiError{
		Code:          fiber.StatusInternalServerError,
		Message:       "internal server error",
		InternalError: err,
	}
}

func UnauthorizedError() ApiError {
	return ApiError{
		Code:    fiber.StatusUnauthorized,
		Message: "unauthorized",
	}
}
