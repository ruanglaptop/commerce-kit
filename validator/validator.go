package validator

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"sort"

	"github.com/go-redis/redis"
	validator "gopkg.in/go-playground/validator.v9"
)

var (
	//ErrUnauthorized declare specific error for granted access on routing
	ErrUnauthorized = fmt.Errorf("Unauthorized")

	//ErrForbidden declare specific error for granted access on forbidden route
	ErrForbidden = fmt.Errorf("Forbidden")
)

// Access object of Access to reflect the Access data for response purpose
// swagger:model
type Access struct {
	ID     int    `json:"id"`
	Name   string `json:"name"`
	Path   string `json:"path"`
	Method string `json:"method"`
	State  string `json:"state"`
	Field  string `json:"field"`
}

// Role object of Role to reflect the role data for response purpose
// swagger:model
type Role struct {
	ID      int       `json:"id"`
	Name    string    `json:"name"`
	Modules []*Module `json:"modules"`
}

// Module object of Access to reflect the Access data for response purpose
// swagger:model
type Module struct {
	ID            int            `json:"id" db:"id"`
	Name          string         `json:"name" db:"name"`
	GroupAccesses []*GroupAccess `json:"groupAccess"`
}

// GroupAccess object of Access to reflect the Access data for response purpose
// swagger:model
type GroupAccess struct {
	ID       int       `json:"id" db:"id"`
	Name     string    `json:"name" db:"name"`
	Accesses []*Access `json:"accesses"`
}

// UserSession represents the user of shopfazz
// swagger:model
type UserSession struct {
	ID           int     `json:"id"`
	Name         string  `json:"name"`
	Roles        []*Role `json:"roles"`
	WarehouseIDs []*int  `json:"warehouseIds"`
}

type Data struct {
	User *UserSession `json:"User"`
}

// ValidatorParamsInterface tolong diisi
type ValidatorParamsInterface interface {
	Struct(s interface{}) error
	RegisterTagNameFunc(fn validator.TagNameFunc)
}

// ValidatorAccessInterface tolong diisi
type ValidatorAccessInterface interface {
	ValidateAccess(params *ValidateAccessParams) error
}

// ValidatorInterface tolong diisi
type ValidatorInterface interface {
	ValidatorParamsInterface
	ValidatorAccessInterface
}

// ValidatorAccess tolong diisi
type ValidatorAccess struct {
	redisClient *redis.Client
}

// ValidateAccessParams object to encapsulate params in validate access
type ValidateAccessParams struct {
	Key              *string
	MethodName       *string
	Path             *string
	CurrentObject    interface{}
	UpdatedObject    interface{}
	WarehouseIDs     *[]int
	IsCurrentService bool
	UserJSONMarshal  *string
}

func sorting(currentObject reflect.Value) {
	data := currentObject.Interface()
	element := currentObject.Index(0)

	if element.Kind() != reflect.Struct {
		sort.Slice(data, func(i, j int) bool {
			if element.Kind() == reflect.Ptr {
				switch currentObject.Index(i).Elem().Kind() {
				case reflect.Int:
					return currentObject.Index(i).Elem().Int() > currentObject.Index(j).Elem().Int()
				case reflect.Float64:
					return currentObject.Index(i).Elem().Float() > currentObject.Index(j).Elem().Float()
				default:
					return currentObject.Index(i).Elem().String() > currentObject.Index(j).Elem().String()
				}
			} else {
				switch currentObject.Index(i).Kind() {
				case reflect.Int:
					return currentObject.Index(i).Int() > currentObject.Index(j).Int()
				case reflect.Float64:
					return currentObject.Index(i).Float() > currentObject.Index(j).Float()
				default:
					return currentObject.Index(i).String() > currentObject.Index(j).String()
				}
			}
		})
	} else {
		sort.Slice(data, func(i, j int) bool {
			if element.Kind() == reflect.Ptr {
				switch currentObject.Index(i).Elem().Field(0).Kind() {
				case reflect.Int:
					return currentObject.Index(i).Elem().Field(0).Int() < currentObject.Index(j).Elem().Field(0).Int()
				case reflect.Float64:
					return currentObject.Index(i).Elem().Field(0).Float() < currentObject.Index(j).Elem().Field(0).Float()
				default:
					return currentObject.Index(i).Elem().Field(0).String() < currentObject.Index(j).Elem().Field(0).String()
				}
			} else {
				switch currentObject.Index(i).Field(0).Kind() {
				case reflect.Int:
					return currentObject.Index(i).Field(0).Int() < currentObject.Index(j).Elem().Field(0).Int()
				case reflect.Float64:
					return currentObject.Index(i).Field(0).Float() < currentObject.Index(j).Elem().Field(0).Float()
				default:
					return currentObject.Index(i).Field(0).String() < currentObject.Index(j).Elem().Field(0).String()
				}
			}
		})
	}
}

func getUpdatedFields(structField reflect.StructField, currentObject reflect.Value, updatedObject reflect.Value, updatedFields map[string]string) map[string]string {
	if (currentObject.Kind() == reflect.Ptr && updatedObject.Kind() == reflect.Ptr) && (currentObject.IsNil() || updatedObject.IsNil()) {
		return updatedFields
	}

	typeMetadata := map[string]interface{}{}
	switch currentObject.Kind() {
	case reflect.Slice:
		updatedLen := updatedObject.Len()
		currentLen := currentObject.Len()
		if updatedLen != currentLen {
			updatedFields[structField.Name] = "updated length"
		} else {
			if updatedLen == 0 {
				break
			}

			if updatedLen > 1 {
				sorting(currentObject)
				sorting(updatedObject)
			}

			for j := 0; j < currentLen; j++ {
				currentElement := currentObject.Index(j)
				updatedElement := updatedObject.Index(j)

				updatedFields = getUpdatedFields(structField, currentElement, updatedElement, updatedFields)
			}
		}
		break
	case reflect.Struct:
		if currentObject.Kind() == reflect.Ptr {
			currentObject = currentObject.Elem()
			updatedObject = updatedObject.Elem()
		}
		for i := 0; i < currentObject.NumField() && i < updatedObject.NumField(); i++ {
			updatedFields = getUpdatedFields(currentObject.Type().Field(i), currentObject.Field(i), updatedObject.Field(i), updatedFields)
		}
		break
	case reflect.Ptr:
		updatedFields = getUpdatedFields(structField, currentObject.Elem(), updatedObject.Elem(), updatedFields)
		break
	case reflect.ValueOf(typeMetadata).Kind():
		break
	default:
		if currentObject.CanInterface() {
			if currentObject.Interface() != updatedObject.Interface() {
				updatedFields[structField.Name] = updatedObject.String()
			}
		}
	}
	return updatedFields
}

func (s *ValidatorAccess) validatePut(warehouseIDMap map[int]*int, params *ValidateAccessParams, accessesMap map[string]map[string]map[string]map[string]*Access) error {
	currentObject := reflect.ValueOf(params.CurrentObject).Elem()
	updatedObject := reflect.ValueOf(params.UpdatedObject).Elem()
	for i := 0; i < currentObject.NumField(); i++ {
		if currentObject.Type().Field(i).Name == "WarehouseID" || currentObject.Type().Field(i).Name == "WarehouseSourceID" {
			id := updatedObject.Field(i).Int()
			if warehouseIDMap[int(id)] == nil {
				return ErrForbidden
			}
		}
		if currentObject.Type().Field(i).Name == "WarehouseIDs" {
			len := updatedObject.Field(i).Len()
			for j := 0; j < len; j++ {
				currentElement := updatedObject.Field(i).Index(j)
				if warehouseIDMap[int(currentElement.Int())] == nil {
					return ErrForbidden
				}
			}
		}
	}
	if accessesMap[*params.MethodName][*params.Path]["all"] != nil {
		return nil
	}

	updatedFields := map[string]string{}
	for i := 0; i < currentObject.NumField(); i++ {
		if currentObject.Type().Field(i).Name == "WarehouseID" || currentObject.Type().Field(i).Name == "WarehouseSourceID" {
			id := updatedObject.Field(i).Int()
			if warehouseIDMap[int(id)] == nil {
				return ErrForbidden
			}
		}
		updatedFields = getUpdatedFields(currentObject.Type().Field(i), currentObject.Field(i), updatedObject.Field(i), updatedFields)
	}

	for field, value := range updatedFields {
		if field != "ID" {
			if accessesMap[*params.MethodName][*params.Path]["field"][field] == nil {
				return ErrForbidden
			}
			if accessesMap[*params.MethodName][*params.Path][field] != nil {
				if accessesMap[*params.MethodName][*params.Path][field][value] == nil {
					return ErrForbidden
				}
			}
		}
	}
	return nil
}

func (s *ValidatorAccess) validatePost(warehouseIDMap map[int]*int, params *ValidateAccessParams, accessesMap map[string]map[string]map[string]map[string]*Access) error {
	if accessesMap[*params.MethodName][*params.Path]["all"] != nil {
		return nil
	}
	currentObject := reflect.ValueOf(params.CurrentObject)
	if currentObject.Kind() == reflect.Ptr {
		currentObject = currentObject.Elem()
	}
	for i := 0; i < currentObject.NumField(); i++ {
		object := currentObject.Field(i)
		if object.Kind() == reflect.Ptr {
			object = object.Elem()
		}
		if object.Kind() == reflect.Slice || object.Kind() == reflect.Struct {
			continue
		}
		if accessesMap[*params.MethodName][*params.Path][currentObject.Type().Field(i).Name] != nil {
			if accessesMap[*params.MethodName][*params.Path][currentObject.Type().Field(i).Name][object.String()] == nil {
				return ErrForbidden
			}
		}
	}
	return nil
}

// ValidateAccess validate access from redis
func (s *ValidatorAccess) ValidateAccess(params *ValidateAccessParams) error {
	if !params.IsCurrentService {
		return nil
	}
	if params.Key == nil {
		return ErrForbidden
	}

	var user UserSession
	var userData Data
	var err error
	userJSONMarshal := ""
	if params.UserJSONMarshal == nil || *params.UserJSONMarshal == "" {
		userJSONMarshal, err = s.redisClient.Get("login:" + *params.Key).Result()
		if err != nil {
			return err
		}
	} else {
		userJSONMarshal = *params.UserJSONMarshal
	}

	if err = json.Unmarshal([]byte(userJSONMarshal), &userData); err != nil {
		return err
	}
	user = *userData.User

	warehouseIDMap := map[int]*int{}
	for _, id := range user.WarehouseIDs {
		warehouseIDMap[*id] = id
	}

	if params.Path == nil {
		if params.WarehouseIDs != nil {
			length := len(*params.WarehouseIDs)
			for i := 0; i < length; i++ {
				if (*params.WarehouseIDs)[i] == 0 {
					for _, id := range warehouseIDMap {
						*params.WarehouseIDs = append(*params.WarehouseIDs, *id)
					}
				} else if warehouseIDMap[(*params.WarehouseIDs)[i]] == nil {
					(*params.WarehouseIDs)[i] = -1
				}
			}
		}
		return nil
	}

	accessesMap := map[string]map[string]map[string]map[string]*Access{}

	for _, role := range user.Roles {
		for _, module := range role.Modules {
			for _, groupAccess := range module.GroupAccesses {
				for _, access := range groupAccess.Accesses {
					if accessesMap[access.Method] == nil {
						accessesMap[access.Method] = map[string]map[string]map[string]*Access{}
					}
					if accessesMap[access.Method][access.Path] == nil {
						accessesMap[access.Method][access.Path] = map[string]map[string]*Access{}
					}
					if accessesMap[access.Method][access.Path][access.State] == nil {
						accessesMap[access.Method][access.Path][access.State] = map[string]*Access{}
					}
					accessesMap[access.Method][access.Path][access.State][access.Field] = access

					if access.Method == *params.MethodName {
						match, _ := regexp.MatchString("^"+access.Path+"$", *params.Path)
						if match {
							params.Path = &access.Path
						}
					}
				}
			}
		}
	}

	if accessesMap[*params.MethodName][*params.Path] == nil {
		return ErrForbidden
	}

	if *params.MethodName == "PUT" {
		if params.CurrentObject == nil {
			return nil
		}
		return s.validatePut(warehouseIDMap, params, accessesMap)
	} else if *params.MethodName == "GET" {
		if params.WarehouseIDs != nil {
			length := len(*params.WarehouseIDs)
			for i := 0; i < length; i++ {
				if (*params.WarehouseIDs)[i] == 0 {
					for _, id := range warehouseIDMap {
						*params.WarehouseIDs = append(*params.WarehouseIDs, *id)
					}
				} else if warehouseIDMap[(*params.WarehouseIDs)[i]] == nil {
					(*params.WarehouseIDs)[i] = -1
				}
			}
		}
		if params.CurrentObject != nil {
			currentObject := reflect.ValueOf(params.CurrentObject)
			if currentObject.Kind() == reflect.Ptr {
				currentObject = currentObject.Elem()
			}
			if currentObject.Kind() == reflect.Struct {
				for i := 0; i < currentObject.NumField(); i++ {
					if accessesMap[*params.MethodName][*params.Path]["all"] != nil {
						return nil
					}
					if accessesMap[*params.MethodName][*params.Path][currentObject.Type().Field(i).Name] != nil {
						if accessesMap[*params.MethodName][*params.Path][currentObject.Type().Field(i).Name][currentObject.Field(i).String()] == nil {
							if currentObject.Field(i).CanSet() && currentObject.Field(i).Kind() == reflect.String {
								currentObject.Field(i).SetString("--")
							}
						}
					}
				}
			}
		}
	} else if *params.MethodName == "POST" || *params.MethodName == "DELETE" {
		if params.WarehouseIDs != nil {
			length := len(*params.WarehouseIDs)
			for i := 0; i < length; i++ {
				if warehouseIDMap[(*params.WarehouseIDs)[i]] == nil {
					return ErrForbidden
				}
			}
		}
		if params.CurrentObject != nil {
			currentObject := reflect.ValueOf(params.CurrentObject)
			if currentObject.Kind() == reflect.Ptr {
				currentObject = currentObject.Elem()
			}
			if currentObject.Kind() == reflect.Struct {
				for i := 0; i < currentObject.NumField(); i++ {
					if currentObject.Type().Field(i).Name == "WarehouseID" || currentObject.Type().Field(i).Name == "WarehouseSourceID" {
						id := currentObject.Field(i).Int()
						if warehouseIDMap[int(id)] == nil {
							return ErrForbidden
						}
					}
					if accessesMap[*params.MethodName][*params.Path][currentObject.Type().Field(i).Name] != nil {
						if accessesMap[*params.MethodName][*params.Path][currentObject.Type().Field(i).Name][currentObject.Field(i).String()] == nil {
							if *params.MethodName == "POST" || *params.MethodName == "DELETE" {
								return ErrForbidden
							}
						}
					}
				}
			} else if currentObject.Kind() == reflect.Int {
				id := currentObject.Int()

				if warehouseIDMap[int(id)] == nil {
					return ErrForbidden
				}
			}

			if *params.MethodName == "POST" {
				return s.validatePost(warehouseIDMap, params, accessesMap)
			}
		}
	}
	return nil
}

// NewValidatorAccess generate validator access object
func NewValidatorAccess(redisClient *redis.Client) *ValidatorAccess {
	return &ValidatorAccess{
		redisClient: redisClient,
	}
}

// Validator tolong diisi
type Validator struct {
	validatorParams ValidatorParamsInterface
	validatorAccess ValidatorAccessInterface
}

// ValidateAccess validate access from redis
func (v *Validator) ValidateAccess(params *ValidateAccessParams) error {
	return v.validatorAccess.ValidateAccess(params)
}

// RegisterTagNameFunc registers a function to get alternate names for StructFields
func (v *Validator) RegisterTagNameFunc(fn validator.TagNameFunc) {
	v.validatorParams.RegisterTagNameFunc(fn)
}

// Struct validates a structs exposed fields, and automatically validates nested structs, unless otherwise specified
func (v *Validator) Struct(s interface{}) error {
	return v.validatorParams.Struct(s)
}

// NewValidator generate validator object
func NewValidator(
	validatorParams ValidatorParamsInterface,
	validatorAccess ValidatorAccessInterface,
) *Validator {
	return &Validator{
		validatorParams: validatorParams,
		validatorAccess: validatorAccess,
	}
}
