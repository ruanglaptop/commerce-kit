package data

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/payfazz/commerce-kit/appcontext"
	"github.com/payfazz/commerce-kit/types"
)

//ErrNotEnough declare specific error for Not Enough
//ErrExisted declare specific error for data already exist
var (
	ErrNotFound     = fmt.Errorf("data is not found")
	ErrAlreadyExist = fmt.Errorf("data already exists")
)

// GenericStorage represents the generic Storage
// for the domain models that matches with its database models
type GenericStorage interface {
	Single(ctx *context.Context, elem interface{}, where string, arg map[string]interface{}) error
	Where(ctx *context.Context, elems interface{}, where string, arg map[string]interface{}) error
	SinglePOSTEMP(ctx *context.Context, elem interface{}, where string, arg map[string]interface{}) error
	WherePOSTEMP(ctx *context.Context, elems interface{}, where string, arg map[string]interface{}) error
	SelectWithQuery(ctx *context.Context, elem interface{}, query string, args map[string]interface{}) error
	FindByID(ctx *context.Context, elem interface{}, id interface{}) error
	FindAll(ctx *context.Context, elems interface{}, page int, limit int, isAsc bool) error
	Insert(ctx *context.Context, elem interface{}) error
	InsertMany(ctx *context.Context, elem interface{}) error
	InsertManyWithTime(ctx *context.Context, elem interface{}, createdAt time.Time) error
	Update(ctx *context.Context, elem interface{}) error
	UpdateMany(ctx *context.Context, elems interface{}) error
	Delete(ctx *context.Context, id interface{}) error
	DeleteMany(ctx *context.Context, ids interface{}) error
	CountAll(ctx *context.Context, count interface{}) error
	HardDelete(ctx *context.Context, id interface{}) error
	ExecQuery(ctx *context.Context, query string, args map[string]interface{}) error
	SelectFirstWithQuery(ctx *context.Context, elem interface{}, query string, args map[string]interface{}) error
}

// ImmutableGenericStorage represents the immutable generic Storage
// for the domain models that matches with its database models.
// The immutable generic Storage provides only the find & insert methods.
type ImmutableGenericStorage interface {
	Single(ctx *context.Context, elem interface{}, where string, arg map[string]interface{}) error
	Where(ctx *context.Context, elems interface{}, where string, arg map[string]interface{}) error
	FindByID(ctx *context.Context, elem interface{}, id interface{}) error
	FindAll(ctx *context.Context, elems interface{}, page int, limit int, isAsc bool) error
	Insert(ctx *context.Context, elem interface{}) error
	DeleteMany(ctx *context.Context, ids interface{}) error
}

// PostgresStorage is the postgres implementation of generic Storage
type PostgresStorage struct {
	db                  Queryer
	tableName           string
	elemType            reflect.Type
	isImmutable         bool
	selectFields        string
	insertFields        string
	insertParams        string
	updateSetFields     string
	updateManySetFields string
	logStorage          LogStorage
}

// LogStorage storage for logs
type LogStorage struct {
	db           Queryer
	logName      string
	elemType     reflect.Type
	insertFields string
	insertParams string
}

// PostgresConfig represents the configuration for the postgres Storage.
type PostgresConfig struct {
	IsImmutable bool
}

// Single queries an element according to the query & argument provided
func (r *PostgresStorage) Single(ctx *context.Context, elem interface{}, where string, arg map[string]interface{}) error {
	db := r.db
	tx, ok := TxFromContext(ctx)
	if ok {
		db = tx
	}
	currentAccount := appcontext.CurrentAccount(ctx)

	if !r.isImmutable {
		where = fmt.Sprintf(`"deletedAt" IS NULL AND %s`, where)
	}
	if currentAccount != nil {
		where = fmt.Sprintf(`"owner" = :currentAccount AND %s`, where)
	}
	arg["currentAccount"] = currentAccount

	statement, err := db.PrepareNamed(fmt.Sprintf(`SELECT %s FROM "%s" WHERE %s`,
		r.selectFields, r.tableName, where))
	if err != nil {
		return err
	}
	defer statement.Close()

	err = statement.Get(elem, arg)
	if err != nil {
		if err == sql.ErrNoRows {
			return ErrNotFound
		}
		return err
	}

	return nil
}

// SinglePOSTEMP queries an element according to the query & argument provided
func (r *PostgresStorage) SinglePOSTEMP(ctx *context.Context, elem interface{}, where string, arg map[string]interface{}) error {
	db := r.db
	tx, ok := TxFromContext(ctx)
	if ok {
		db = tx
	}
	currentAccount := appcontext.CurrentAccount(ctx)

	if !r.isImmutable {
		where = fmt.Sprintf(`"deletedAt" IS NULL AND %s`, where)
	}
	if currentAccount != nil {
		where = fmt.Sprintf(`("userId" = :currentAccount OR "owner" = :currentAccount) AND %s`, where)
	}
	arg["currentAccount"] = currentAccount

	statement, err := db.PrepareNamed(fmt.Sprintf(`SELECT %s FROM "%s" WHERE %s`,
		r.selectFields, r.tableName, where))
	if err != nil {
		return err
	}
	defer statement.Close()

	err = statement.Get(elem, arg)
	if err != nil {
		if err == sql.ErrNoRows {
			return ErrNotFound
		}
		return err
	}

	return nil
}

// Where queries the elements according to the query & argument provided
func (r *PostgresStorage) Where(ctx *context.Context, elems interface{}, where string, arg map[string]interface{}) error {
	db := r.db
	tx, ok := TxFromContext(ctx)
	if ok {
		db = tx
	}
	currentAccount := appcontext.CurrentAccount(ctx)

	if !r.isImmutable {
		where = fmt.Sprintf(`"deletedAt" IS NULL AND %s`, where)
	}
	if currentAccount != nil {
		where = fmt.Sprintf(`"owner" = :currentAccount AND %s`, where)
	}
	arg["currentAccount"] = currentAccount

	query := fmt.Sprintf(`SELECT %s FROM "%s" WHERE %s`, r.selectFields, r.tableName, where)
	query, args, err := sqlx.Named(query, arg)
	if err != nil {
		return err
	}

	query, args, err = sqlx.In(query, args...)
	if err != nil {
		return err
	}

	query = db.Rebind(query)

	err = db.Select(elems, query, args...)
	if err != nil {
		return err
	}

	return nil
}

// WherePOSTEMP queries the elements according to the query & argument provided
func (r *PostgresStorage) WherePOSTEMP(ctx *context.Context, elems interface{}, where string, arg map[string]interface{}) error {
	db := r.db
	tx, ok := TxFromContext(ctx)
	if ok {
		db = tx
	}
	currentAccount := appcontext.CurrentAccount(ctx)

	if !r.isImmutable {
		where = fmt.Sprintf(`"deletedAt" IS NULL AND %s`, where)
	}
	if currentAccount != nil {
		where = fmt.Sprintf(`("userId" = :currentAccount OR "owner" = :currentAccount) AND %s`, where)
	}
	arg["currentAccount"] = currentAccount

	query := fmt.Sprintf(`SELECT %s FROM "%s" WHERE %s`, r.selectFields, r.tableName, where)
	query, args, err := sqlx.Named(query, arg)
	if err != nil {
		return err
	}

	query, args, err = sqlx.In(query, args...)
	if err != nil {
		return err
	}

	query = db.Rebind(query)

	err = db.Select(elems, query, args...)
	if err != nil {
		return err
	}

	return nil
}

// SelectWithQuery Customizable Query for Select
func (r *PostgresStorage) SelectWithQuery(ctx *context.Context, elems interface{}, query string, arg map[string]interface{}) error {
	db := r.db
	tx, ok := TxFromContext(ctx)
	if ok {
		db = tx
	}

	query, args, err := sqlx.Named(query, arg)
	if err != nil {
		return err
	}

	query, args, err = sqlx.In(query, args...)
	if err != nil {
		return err
	}

	query = db.Rebind(query)

	err = db.Select(elems, query, args...)
	if err != nil {
		if err == sql.ErrNoRows {
			return ErrNotFound
		}
		return err
	}

	return nil
}

// FindByID finds an element by its id
// it's defined in this project context that
// the element id column in the db should be "id"
func (r *PostgresStorage) FindByID(ctx *context.Context, elem interface{}, id interface{}) error {
	where := `"id" = :id`
	err := r.Single(ctx, elem, where, map[string]interface{}{
		"id": id,
	})
	if err != nil {
		return err
	}

	return nil
}

// FindAll finds all elements from the database.
func (r *PostgresStorage) FindAll(ctx *context.Context, elems interface{}, page int, limit int, isAsc bool) error {
	where := `true`
	where = fmt.Sprintf(`%s ORDER BY "id"`, where)

	if !isAsc {
		where = fmt.Sprintf(`%s %s`, where, "DESC")
	}

	where = fmt.Sprintf(`%s LIMIT :limit OFFSET :offset`, where)

	err := r.Where(ctx, elems, where, map[string]interface{}{
		"limit":  limit,
		"offset": (page - 1) * limit,
	})

	if err != nil {
		return err
	}

	return nil
}

func interfaceConversion(i interface{}) (map[string]interface{}, error) {
	resJSON, err := json.Marshal(i)
	if err != nil {
		return nil, err
	}
	var res map[string]interface{}
	err = json.Unmarshal(resJSON, &res)
	if err != nil {
		return nil, err
	}

	return res, nil
}

// Insert inserts a new element into the database.
// It assumes the primary key of the table is "id" with serial type.
// It will set the "owner" field of the element with the current account in the context if exists.
// It will set the "createdAt" and "updatedAt" fields with current time.
// If immutable set true, it won't insert the updatedAt
func (r *PostgresStorage) Insert(ctx *context.Context, elem interface{}) error {
	currentAccount := appcontext.CurrentAccount(ctx)
	currentUserID, currentUserType := determineUser(ctx)
	db := r.db
	tx, ok := TxFromContext(ctx)
	if ok {
		db = tx
	}

	statement, err := db.PrepareNamed(fmt.Sprintf(`
		INSERT INTO "%s"(%s)
		VALUES (%s)
		RETURNING %s`, r.tableName, r.insertFields, r.insertParams, r.selectFields))
	if err != nil {
		return err
	}
	defer statement.Close()

	dbArgs := r.insertArgs(currentAccount, currentUserID, elem, 0)
	err = statement.Get(elem, dbArgs)
	if err != nil {
		return err
	}

	elemID := r.findID(elem)
	now := time.Now()

	valueAfter, err := interfaceConversion(elem)
	if err != nil {
		return err
	}
	err = r.createLog(ctx, &ActivityLog{
		UserID:          currentUserID,
		UserType:        currentUserType,
		TableName:       r.tableName,
		ReferenceID:     elemID.(int),
		Metadata:        map[string]interface{}{},
		ValueBefore:     nil,
		ValueAfter:      valueAfter,
		TransactionTime: &now,
		TransactionType: "Insert",
	})
	if err != nil {
		fmt.Printf("\nError while write activitylog: %v\n", err)
	}

	return nil
}

func (r *PostgresStorage) insertArgs(currentAccount *int, currentUserID int, elem interface{}, index int) map[string]interface{} {
	res := map[string]interface{}{
		"owner":     currentAccount,
		"createdAt": time.Now().UTC(),
		"createdBy": currentUserID,
	}

	if !r.isImmutable {
		res["updatedAt"] = time.Now().UTC()
		res["updatedBy"] = currentUserID
	}

	var v reflect.Value
	if reflect.TypeOf(elem) == reflect.TypeOf(reflect.Value{}) {
		data := elem.(reflect.Value)
		v = reflect.Indirect(data)
	} else {
		v = reflect.ValueOf(elem).Elem()
	}

	for i := 0; i < v.NumField(); i++ {
		dbTag := r.elemType.Field(i).Tag.Get("db")
		if !readOnlyTag(dbTag) && !emptyTag(dbTag) {
			var typeMapString map[string]interface{}
			var val interface{}
			if v.Field(i).Type() == reflect.TypeOf(typeMapString) {
				metadataBytes, err := json.Marshal(v.Field(i).Interface())
				if err != nil {
					val = "{}"
				} else {
					val = string(metadataBytes)
				}
			} else {
				val = v.Field(i).Interface()
			}
			res[dbTag] = val
		}
	}

	if index != 0 {
		s := strconv.Itoa(index)
		res = renamingKey(res, s)
	}

	return res
}

func (r *LogStorage) insertArgs(currentAccount *int, currentUserID int, elem interface{}, index int) map[string]interface{} {
	res := map[string]interface{}{
		"owner":     currentAccount,
		"createdAt": time.Now().UTC(),
		"createdBy": currentUserID,
	}

	var v reflect.Value

	if reflect.TypeOf(elem) == reflect.TypeOf(reflect.Value{}) {
		data := elem.(reflect.Value)
		v = reflect.Indirect(data)
	} else {
		v = reflect.ValueOf(elem).Elem()
	}

	for i := 0; i < v.NumField(); i++ {
		dbTag := r.elemType.Field(i).Tag.Get("db")
		if !readOnlyTag(dbTag) && !emptyTag(dbTag) {
			var typeMapString map[string]interface{}
			var val interface{}
			if v.Field(i).Type() == reflect.TypeOf(typeMapString) {
				metadataBytes, err := json.Marshal(v.Field(i).Interface())
				if err != nil {
					val = "{}"
				} else {
					val = string(metadataBytes)
				}
			} else {
				val = v.Field(i).Interface()
			}
			res[dbTag] = val
		}
	}

	if index != 0 {
		s := strconv.Itoa(index)
		res = renamingKey(res, s)
	}

	return res
}

// InsertMany is function for creating many datas into specific table in database.
func (r *PostgresStorage) InsertMany(ctx *context.Context, elem interface{}) error {
	currentAccount := appcontext.CurrentAccount(ctx)
	currentUserID, _ := determineUser(ctx)
	db := r.db
	tx, ok := TxFromContext(ctx)
	if ok {
		db = tx
	}

	sqlStr := fmt.Sprintf(`
	INSERT INTO "%s"(%s)
	VALUES `, r.tableName, r.insertFields)

	var dbArgs map[string]interface{}

	datas := reflect.ValueOf(elem)

	insertFields := strings.Split(r.insertFields, ",")
	limit := 60000 / len(insertFields)
	indexData := 0
	if datas.Kind() == reflect.Slice {
		for i := 0; i < datas.Len(); i++ {
			sqlStr += fmt.Sprintf("(%s),", insertParams(r.elemType, r.isImmutable, i+1))
			arg := r.insertArgs(currentAccount, currentUserID, datas.Index(i), i+1)
			if indexData == 0 {
				dbArgs = arg
			} else {
				for k, v := range arg {
					dbArgs[k] = v
				}
			}
			indexData++
			if indexData == limit {
				err := r.insertData(ctx, sqlStr, dbArgs)
				if err != nil {
					return err
				}

				indexData = 0
				sqlStr = fmt.Sprintf(`
				INSERT INTO "%s"(%s)
				VALUES `, r.tableName, r.insertFields)
				dbArgs = map[string]interface{}{}
			}
		}
	}

	if datas.Kind() == reflect.Map {
		for key, element := range datas.MapKeys() {
			sqlStr += fmt.Sprintf("(%s),", insertParams(r.elemType, r.isImmutable, key+1))
			arg := r.insertArgs(currentAccount, currentUserID, datas.MapIndex(element), key+1)
			if indexData == 0 {
				dbArgs = arg
			} else {
				for k, v := range arg {
					dbArgs[k] = v
				}
			}
			indexData++
			if indexData == limit {
				err := r.insertData(ctx, sqlStr, dbArgs)
				if err != nil {
					return err
				}

				indexData = 0
				sqlStr = fmt.Sprintf(`
				INSERT INTO "%s"(%s)
				VALUES `, r.tableName, r.insertFields)
				dbArgs = map[string]interface{}{}
			}
		}
	}

	sqlStr = strings.TrimSuffix(sqlStr, ",")
	sqlStr += fmt.Sprintf(" RETURNING %s", r.selectFields)

	statement, err := db.PrepareNamed(sqlStr)
	if err != nil {
		return err
	}
	defer statement.Close()

	_, err = statement.Get(dbArgs)
	if err != nil {
		return err
	}

	return nil
}

// InsertManyWithTime is function for creating many datas into specific table in database with specific createdAt.
func (r *PostgresStorage) InsertManyWithTime(ctx *context.Context, elem interface{}, createdAt time.Time) error {
	currentAccount := appcontext.CurrentAccount(ctx)
	currentUserID, _ := determineUser(ctx)

	sqlStr := fmt.Sprintf(`
	INSERT INTO "%s"(%s)
	VALUES `, r.tableName, r.insertFields)

	var dbArgs map[string]interface{}

	datas := reflect.ValueOf(elem)

	a := strings.Split(r.insertFields, ",")
	limit := 60000 / len(a)
	indexData := 0
	if datas.Kind() == reflect.Slice {
		for i := 0; i < datas.Len(); i++ {
			sqlStr += fmt.Sprintf("(%s),", insertParams(r.elemType, r.isImmutable, i+1))

			arg := r.insertArgs(currentAccount, currentUserID, datas.Index(i), i+1)
			arg[fmt.Sprintf("createdAt%d", i+1)] = createdAt
			if indexData == 0 {
				dbArgs = arg
			} else {
				for k, v := range arg {
					dbArgs[k] = v
				}
			}
			indexData++
			if indexData == limit {
				err := r.insertData(ctx, sqlStr, dbArgs)
				if err != nil {
					return err
				}

				indexData = 0
				sqlStr = fmt.Sprintf(`
				INSERT INTO "%s"(%s)
				VALUES `, r.tableName, r.insertFields)
				dbArgs = map[string]interface{}{}
			}
		}
	}

	if datas.Kind() == reflect.Map {
		for key, element := range datas.MapKeys() {
			sqlStr += fmt.Sprintf("(%s),", insertParams(r.elemType, r.isImmutable, key+1))
			arg := r.insertArgs(currentAccount, currentUserID, datas.MapIndex(element), key+1)
			arg[fmt.Sprintf("createdAt%d", key+1)] = createdAt
			if indexData == 0 {
				dbArgs = arg
			} else {
				for k, v := range arg {
					dbArgs[k] = v
				}
			}
			indexData++
			if indexData == limit {
				err := r.insertData(ctx, sqlStr, dbArgs)
				if err != nil {
					return err
				}

				indexData = 0
				sqlStr = fmt.Sprintf(`
				INSERT INTO "%s"(%s)
				VALUES `, r.tableName, r.insertFields)
				dbArgs = map[string]interface{}{}
			}
		}
	}

	err := r.insertData(ctx, sqlStr, dbArgs)
	if err != nil {
		return err
	}

	return nil
}

func (r *PostgresStorage) insertData(ctx *context.Context, sqlStr string, dbArgs map[string]interface{}) error {
	db := r.db
	tx, ok := TxFromContext(ctx)
	if ok {
		db = tx
	}

	sqlStr = strings.TrimSuffix(sqlStr, ",")
	sqlStr += fmt.Sprintf(" RETURNING %s", r.selectFields)

	statement, err := db.PrepareNamed(sqlStr)
	if err != nil {
		return err
	}
	defer statement.Close()

	_, err = statement.Get(dbArgs)
	if err != nil {
		return err
	}

	return nil
}

// RenamingKey is function for renaming key for map
func renamingKey(m map[string]interface{}, add string) map[string]interface{} {
	newMap := map[string]interface{}{}
	for k, v := range m {
		newKey := fmt.Sprint(k, add)
		newMap[newKey] = v
	}
	return newMap
}

func (r *PostgresStorage) findChanges(existingElem interface{}, elem interface{}) map[string]interface{} {
	diff := map[string]interface{}{}
	ev := reflect.ValueOf(existingElem).Elem()
	v := reflect.ValueOf(elem).Elem()
	for i := 0; i < ev.NumField(); i++ {
		dbTag := r.elemType.Field(i).Tag.Get("db")
		if !readOnlyTag(dbTag) && !emptyTag(dbTag) {
			val1 := ev.Field(i).Interface()
			val2 := v.Field(i).Interface()
			if !reflect.DeepEqual(val1, val2) {

				singleDiff := make([]interface{}, 2)
				singleDiff[0] = val1
				singleDiff[1] = val2
				diff[dbTag] = singleDiff
			}
		}
	}
	return diff
}

// Update updates the element in the database.
// It will update the "updatedAt" field.
func (r *PostgresStorage) Update(ctx *context.Context, elem interface{}) error {
	currentUserID, currentUserType := determineUser(ctx)
	db := r.db
	tx, ok := TxFromContext(ctx)
	if ok {
		db = tx
	}
	id := r.findID(elem)
	existingElem := reflect.New(r.elemType).Interface()
	err := r.FindByID(ctx, existingElem, id)
	if err != nil {
		return err
	}

	statement, err := db.PrepareNamed(fmt.Sprintf(`
		UPDATE "%s" SET %s WHERE "id" = :id RETURNING %s`,
		r.tableName,
		r.updateSetFields,
		r.selectFields))
	if err != nil {
		return err
	}
	defer statement.Close()

	updateArgs := r.updateArgs(currentUserID, existingElem, elem)
	updateArgs["id"] = id
	err = statement.Get(elem, updateArgs)
	if err != nil {
		return err
	}
	elemID := r.findID(elem)
	if err != nil {
		return err
	}
	now := time.Now()

	valueBefore, err := interfaceConversion(existingElem)
	if err != nil {
		return err
	}
	valueAfter, err := interfaceConversion(elem)
	if err != nil {
		return err
	}

	err = r.createLog(ctx, &ActivityLog{
		UserID:          currentUserID,
		UserType:        currentUserType,
		TableName:       r.tableName,
		ReferenceID:     elemID.(int),
		Metadata:        r.findChanges(existingElem, elem),
		ValueBefore:     valueBefore,
		ValueAfter:      valueAfter,
		TransactionTime: &now,
		TransactionType: "Update",
	})
	if err != nil {
		fmt.Printf("\nError while write activitylog: %v\n", err)
	}

	return nil
}

// UpdateMany updates the element in the database.
// It will update the "updatedAt" field.
func (r *PostgresStorage) UpdateMany(ctx *context.Context, elems interface{}) error {
	currentUserID, _ := determineUser(ctx)
	db := r.db
	tx, ok := TxFromContext(ctx)
	if ok {
		db = tx
	}

	dbArgs := map[string]interface{}{}

	sqlStr := fmt.Sprintf(`
	UPDATE "%s" as "currentTable" 
	SET
		%s
	FROM (VALUES
	`, r.tableName, r.updateManySetFields)

	datas := reflect.ValueOf(elems)

	limit := 2000
	indexData := 0
	if datas.Kind() == reflect.Slice {
		for i := 0; i < datas.Len(); i++ {
			sqlStrIndex, arg := r.updateManyParams(currentUserID, datas.Index(i), i+1)
			sqlStr += sqlStrIndex
			if indexData == 0 {
				dbArgs = arg
			} else {
				for k, v := range arg {
					dbArgs[k] = v
				}
			}
			indexData++
			if indexData == limit {
				err := r.updateData(ctx, sqlStr, dbArgs)
				if err != nil {
					return err
				}

				indexData = 0
				sqlStr = fmt.Sprintf(`
				UPDATE "%s" as "currentTable" 
				SET
					%s
				FROM (VALUES
				`, r.tableName, r.updateManySetFields)
				dbArgs = map[string]interface{}{}
			}
		}
	}

	if datas.Kind() == reflect.Map {
		for key, element := range datas.MapKeys() {
			sqlStrIndex, arg := r.updateManyParams(currentUserID, datas.MapIndex(element), key+1)
			sqlStr += sqlStrIndex
			if indexData == 0 {
				dbArgs = arg
			} else {
				for k, v := range arg {
					dbArgs[k] = v
				}
			}
			indexData++
			if indexData == limit {
				err := r.updateData(ctx, sqlStr, dbArgs)
				if err != nil {
					return err
				}

				indexData = 0
				sqlStr = fmt.Sprintf(`
				UPDATE "%s" as "currentTable" 
				SET
					%s
				FROM (VALUES
				`, r.tableName, r.updateManySetFields)
				dbArgs = map[string]interface{}{}
			}
		}
	}

	sqlStr = strings.TrimSuffix(sqlStr, ",")

	sqlStr = fmt.Sprintf(`%s
	) as "updatedTable"("updatedAt", "updatedBy", %s)
	where cast("currentTable".id as int) = cast("updatedTable".id as int)
	RETURNING %s
	`, sqlStr, r.selectFields, r.selectFields)

	statement, err := db.PrepareNamed(sqlStr)
	if err != nil {
		return err
	}
	defer statement.Close()

	_, err = statement.Get(dbArgs)
	if err != nil {
		return err
	}

	return nil
}

func (r *PostgresStorage) updateData(ctx *context.Context, sqlStr string, dbArgs map[string]interface{}) error {
	db := r.db
	tx, ok := TxFromContext(ctx)
	if ok {
		db = tx
	}

	sqlStr = strings.TrimSuffix(sqlStr, ",")

	sqlStr = fmt.Sprintf(`%s
	) as "updatedTable"("updatedAt", "updatedBy", %s)
	where cast("currentTable".id as int) = cast("updatedTable".id as int)
	RETURNING %s
	`, sqlStr, r.selectFields, r.selectFields)

	statement, err := db.PrepareNamed(sqlStr)
	if err != nil {
		return err
	}
	defer statement.Close()

	_, err = statement.Get(dbArgs)
	if err != nil {
		return err
	}

	return nil
}

func (r *PostgresStorage) updateManyParams(currentUserID int, elem interface{}, index int) (string, map[string]interface{}) {
	sqlStr := fmt.Sprintf(`(cast(:updatedAt%d as timestamp),%d,`, index, currentUserID)

	var v reflect.Value

	res := map[string]interface{}{
		"updatedAt": time.Now().UTC().Format(time.RFC3339),
	}

	if reflect.TypeOf(elem) == reflect.TypeOf(reflect.Value{}) {
		data := elem.(reflect.Value)
		v = reflect.Indirect(data)
	} else {
		v = reflect.ValueOf(elem).Elem()
	}

	for i := 0; i < v.NumField(); i++ {
		dbTag := r.elemType.Field(i).Tag.Get("db")
		if !emptyTag(dbTag) {
			var typeMapString types.Metadata
			var val interface{}
			var typeTime time.Time
			var field reflect.Value
			if v.Field(i).Kind() == reflect.Ptr {
				field = v.Field(i).Elem()
			} else {
				field = v.Field(i)
			}
			if field.Type() == reflect.TypeOf(typeMapString) {
				metadataBytes, err := json.Marshal(field.Interface())
				if err != nil {
					val = "{}"
				} else {
					val = string(metadataBytes)
				}
			} else {
				val = field.Interface()
			}

			if dbTag == "createdAt" || dbTag == "updatedAt" || field.Type() == reflect.TypeOf(typeTime) {
				valTime := val.(time.Time)
				val = valTime.Format(time.RFC3339)
				sqlStr += fmt.Sprintf(`cast(:%s%d as timestamp),`, dbTag, index)
				res[dbTag] = val
			} else if field.Type() == reflect.TypeOf(typeMapString) {
				sqlStr += fmt.Sprintf(`cast(:%s%d as jsonb),`, dbTag, index)
				res[dbTag] = val
			} else {
				switch field.Kind() {
				case reflect.String:
					if r.elemType.Field(i).Tag.Get("cast") != "" {
						sqlStr += fmt.Sprintf(`cast('%s' as %s),`, val, r.elemType.Field(i).Tag.Get("cast"))
					} else {
						sqlStr += fmt.Sprintf(`'%s',`, val)
					}
					break
				default:
					sqlStr += fmt.Sprint(val, ",")
					break
				}
			}
		}
	}

	sqlStr = strings.TrimSuffix(sqlStr, ",")

	sqlStr += "),"

	if index != 0 {
		s := strconv.Itoa(index)
		res = renamingKey(res, s)
	}

	return sqlStr, res
}

// it assumes the id column named "id"
func (r *PostgresStorage) findID(elem interface{}) interface{} {
	v := reflect.ValueOf(elem).Elem()
	for i := 0; i < v.NumField(); i++ {
		dbTag := r.elemType.Field(i).Tag.Get("db")
		if idTag(dbTag) {
			return v.Field(i).Interface()
		}
	}
	return nil
}

func (r *PostgresStorage) updateArgs(currentUserID int, existingElem interface{}, elem interface{}) map[string]interface{} {
	res := map[string]interface{}{
		"updatedAt": time.Now().UTC(),
		"updatedBy": currentUserID,
	}

	v := reflect.ValueOf(elem).Elem()
	ev := reflect.ValueOf(existingElem).Elem()
	for i := 0; i < ev.NumField(); i++ {
		dbTag := r.elemType.Field(i).Tag.Get("db")
		if !readOnlyTag(dbTag) && !emptyTag(dbTag) {
			var typeMapString map[string]interface{}
			var val interface{}

			if v.Field(i).Type() == reflect.TypeOf(typeMapString) {
				metadataBytes, err := json.Marshal(v.Field(i).Interface())
				if err != nil {
					val = "{}"
				} else {
					val = string(metadataBytes)
				}
			} else {
				val = v.Field(i).Interface()
			}
			res[dbTag] = val
		}
	}
	return res
}

// Delete deletes the elem from database.
// Delete not really deletes the elem from the db, but it will set the
// "deletedAt" column to current time.
func (r *PostgresStorage) Delete(ctx *context.Context, id interface{}) error {
	currentUser := appcontext.UserID(ctx)
	db := r.db
	tx, ok := TxFromContext(ctx)
	if ok {
		db = tx
	}

	statement, err := db.PrepareNamed(fmt.Sprintf(`
		UPDATE "%s" SET "deletedAt" = :deletedAt, "deletedBy" = :deletedBy WHERE "id" = :id RETURNING %s
	`, r.tableName, r.selectFields))
	if err != nil {
		return err
	}
	defer statement.Close()

	deleteArgs := map[string]interface{}{
		"id":        id,
		"deletedAt": time.Now().UTC(),
		"deletedBy": currentUser,
	}
	now := time.Now()
	currentUserID, currentUserType := determineUser(ctx)

	err = r.createLog(ctx, &ActivityLog{
		UserID:          currentUserID,
		UserType:        currentUserType,
		TableName:       r.tableName,
		ReferenceID:     id.(int),
		Metadata:        map[string]interface{}{},
		ValueBefore:     nil,
		ValueAfter:      nil,
		TransactionTime: &now,
		TransactionType: "Delete",
	})
	if err != nil {
		fmt.Printf("\nError while write activitylog: %v\n", err)
	}

	_, err = statement.Exec(deleteArgs)
	if err != nil {
		return err
	}

	return nil
}

// DeleteMany delete elems from database.
// DeleteMany not really delete elems from the db, but it will set the
// "deletedAt" column to current time.
func (r *PostgresStorage) DeleteMany(ctx *context.Context, ids interface{}) error {
	db := r.db
	tx, ok := TxFromContext(ctx)
	if ok {
		db = tx
	}

	// Check if interface is type of slices
	datas := reflect.ValueOf(ids)
	if datas.Kind() != reflect.Slice {
		return fmt.Errorf("ids data should be slices")
	}

	if r.isImmutable {
		query := fmt.Sprintf(`DELETE FROM "%s" WHERE "id" IN (:ids)`, r.tableName)
		query, args, err := sqlx.Named(query, map[string]interface{}{
			"ids": ids,
		})
		if err != nil {
			return err
		}

		query, args, err = sqlx.In(query, args...)
		if err != nil {
			return err
		}

		query = db.Rebind(query)
		db.MustExec(query, args...)
		return nil
	}

	var queryParam string
	var payloads = map[string]interface{}{
		"deletedAt": time.Now().UTC(),
	}

	for i := 0; i < datas.Len(); i++ {
		queryParam += fmt.Sprintf(":%s%d,", "id", i+1)
		payloads[fmt.Sprintf("%s%d", "id", i+1)] = datas.Index(i).Interface()
	}

	queryParam = strings.TrimSuffix(queryParam, ",")

	statement, err := db.PrepareNamed(fmt.Sprintf(`
		UPDATE "%s" SET "deletedAt" = :deletedAt WHERE "id" in (%s) RETURNING %s
	`, r.tableName, queryParam, r.selectFields))
	if err != nil {
		return err
	}

	_, err = statement.Exec(payloads)
	if err != nil {
		return err
	}
	return nil
}

// CountAll is function to count all row datas in specific table in database
func (r *PostgresStorage) CountAll(ctx *context.Context, count interface{}) error {
	var where string
	db := r.db
	tx, ok := TxFromContext(ctx)
	if ok {
		db = tx
	}

	if !r.isImmutable {
		where = fmt.Sprintf(`"deletedAt" IS NULL`)
	}

	q := fmt.Sprintf(`SELECT COUNT(*) FROM "%s" WHERE %s`, r.tableName, where)

	err := db.Get(count, q)
	if err != nil {
		if err == sql.ErrNoRows {
			return ErrNotFound
		}
		return err
	}

	return nil
}

// HardDelete is function to hard deleting data into specific table in database
func (r *PostgresStorage) HardDelete(ctx *context.Context, id interface{}) error {
	db := r.db
	tx, ok := TxFromContext(ctx)
	if ok {
		db = tx
	}

	statement, err := db.PrepareNamed(fmt.Sprintf(`
		DELETE FROM "%s" WHERE "id" = :id
	`, r.tableName))
	if err != nil {
		return err
	}
	defer statement.Close()

	deleteArgs := map[string]interface{}{
		"id": id,
	}
	_, err = statement.Exec(deleteArgs)
	if err != nil {
		return err
	}

	return nil
}

// ExecQuery is function to only execute raw query into database
func (r *PostgresStorage) ExecQuery(ctx *context.Context, query string, args map[string]interface{}) error {
	db := r.db
	tx, ok := TxFromContext(ctx)
	if ok {
		db = tx
	}

	statement, err := db.PrepareNamed(query)
	if err != nil {
		return err
	}
	defer statement.Close()

	_, err = statement.Exec(args)
	if err != nil {
		return err
	}

	return nil
}

// SelectFirstWithQuery Customizable Query for Select only take the first row
func (r *PostgresStorage) SelectFirstWithQuery(ctx *context.Context, elems interface{}, query string, arg map[string]interface{}) error {
	db := r.db
	tx, ok := TxFromContext(ctx)
	if ok {
		db = tx
	}

	query, args, err := sqlx.Named(query, arg)
	if err != nil {
		return err
	}

	query, args, err = sqlx.In(query, args...)
	if err != nil {
		return err
	}

	query = db.Rebind(query)

	err = db.Get(elems, query, args...)
	if err != nil {
		if err == sql.ErrNoRows {
			return ErrNotFound
		}
		return err
	}

	return nil
}

// ActivityLog log for transactions (insert, update, delete)
type ActivityLog struct {
	ID              int                    `db:"id"`
	UserID          int                    `db:"userId"`
	UserType        string                 `db:"userType"`
	TableName       string                 `db:"tableName"`
	ReferenceID     int                    `db:"referenceId"`
	Metadata        map[string]interface{} `db:"metadata"`
	ValueBefore     map[string]interface{} `db:"valueBefore"`
	ValueAfter      map[string]interface{} `db:"valueAfter"`
	TransactionTime *time.Time             `db:"transactionTime"`
	TransactionType string                 `db:"transactionType"`
	CreatedAt       *time.Time             `db:"createdAt"`
}

func (r *PostgresStorage) createLog(ctx *context.Context, params *ActivityLog) error {
	currentAccount := appcontext.CurrentAccount(ctx)
	if currentAccount == nil {
		return nil
	}
	db := r.logStorage.db
	tx, ok := TxFromContext(ctx)
	if ok {
		db = tx
	}

	statement, err := db.PrepareNamed(
		fmt.Sprintf(`
		INSERT INTO "%s"(%s)
		VALUES (%s)
		`, r.logStorage.logName, r.logStorage.insertFields, r.logStorage.insertParams),
	)

	if err != nil {
		return err
	}
	defer statement.Close()

	dbArgs := r.logStorage.insertArgs(currentAccount, params.UserID, params, 0)

	_, err = statement.Exec(dbArgs)
	if err != nil {
		return err
	}
	return nil
}

func getContextVariables(ctx *context.Context) (*int, int, *int) {
	return appcontext.UserID(ctx), appcontext.CustomerID(ctx), appcontext.ClientID(ctx)
}

func determineUser(ctx *context.Context) (int, string) {
	userID, customerID, clientID := getContextVariables(ctx)
	var resUserID int
	var resUserType string
	if userID != nil {
		resUserID = *userID
		resUserType = "User"
	} else if customerID != 0 {
		resUserID = customerID
		resUserType = "Customer"
	} else if clientID != nil && *clientID != 0 {
		resUserID = *clientID
		resUserType = "Client"
	} else {
		resUserID = 0
		resUserType = "System"
	}
	return resUserID, resUserType
}

// NewLogStorage creates a logStorage
func NewLogStorage(db *sqlx.DB, logName string) *LogStorage {
	logType := reflect.TypeOf(ActivityLog{})
	return &LogStorage{
		db:           db,
		logName:      logName,
		elemType:     logType,
		insertFields: insertFields(logType, true),
		insertParams: insertParams(logType, true, 0),
	}
}

// NewPostgresStorage creates a new generic postgres Storage
func NewPostgresStorage(db *sqlx.DB, tableName string, elem interface{}, cfg PostgresConfig, logStorage LogStorage) *PostgresStorage {
	elemType := reflect.TypeOf(elem)
	return &PostgresStorage{
		db:                  db,
		tableName:           tableName,
		elemType:            elemType,
		isImmutable:         cfg.IsImmutable,
		selectFields:        selectFields(elemType),
		insertFields:        insertFields(elemType, cfg.IsImmutable),
		insertParams:        insertParams(elemType, cfg.IsImmutable, 0),
		updateSetFields:     updateSetFields(elemType),
		updateManySetFields: updateManySetFields(elemType),
		logStorage:          logStorage,
	}
}

func selectFields(elemType reflect.Type) string {
	dbFields := []string{}
	for i := 0; i < elemType.NumField(); i++ {
		field := elemType.Field(i)
		dbTag := field.Tag.Get("db")
		if dbTag != "" && dbTag != "-" {
			dbFields = append(dbFields, fmt.Sprintf("\"%s\"", dbTag))
		}
	}
	return strings.Join(dbFields, ",")
}

func insertFields(elemType reflect.Type, isImmutable bool) string {
	dbFields := []string{"\"owner\"", "\"createdAt\"", "\"createdBy\""}
	if !isImmutable {
		dbFields = append(dbFields, "\"updatedAt\"", "\"updatedBy\"")
	}
	for i := 0; i < elemType.NumField(); i++ {
		field := elemType.Field(i)
		dbTag := field.Tag.Get("db")
		if !readOnlyTag(dbTag) && !emptyTag(dbTag) {
			dbFields = append(dbFields, fmt.Sprintf("\"%s\"", dbTag))
		}
	}
	return strings.Join(dbFields, ",")
}

func insertParams(elemType reflect.Type, isImmutable bool, index int) string {
	dbParams := []string{":owner", ":createdAt", ":createdBy"}
	if !isImmutable {
		dbParams = append(dbParams, ":updatedAt", ":updatedBy")
	}
	for i := 0; i < elemType.NumField(); i++ {
		field := elemType.Field(i)
		dbTag := field.Tag.Get("db")
		if !readOnlyTag(dbTag) && !emptyTag(dbTag) {
			dbParams = append(dbParams, fmt.Sprintf(":%s", dbTag))
		}
	}

	if index != 0 {
		s := strconv.Itoa(index)
		for i, v := range dbParams {
			dbParams[i] = fmt.Sprint(v, s)
		}
	}

	return strings.Join(dbParams, ",")
}

func updateSetFields(elemType reflect.Type) string {
	setFields := []string{"\"updatedAt\" = :updatedAt", "\"updatedBy\" = :updatedBy"}
	for i := 0; i < elemType.NumField(); i++ {
		field := elemType.Field(i)
		dbTag := field.Tag.Get("db")
		if !readOnlyTag(dbTag) && !emptyTag(dbTag) {
			setFields = append(setFields, fmt.Sprintf("\"%s\" = :%s", dbTag, dbTag))
		}
	}
	return strings.Join(setFields, ",")
}

func updateManySetFields(elemType reflect.Type) string {
	setManyFields := []string{`"updatedAt" = "updatedTable"."updatedAt"`, `"updatedBy" = "updatedTable"."updatedBy"`}
	for i := 0; i < elemType.NumField(); i++ {
		field := elemType.Field(i)
		dbTag := field.Tag.Get("db")
		if !readOnlyTag(dbTag) && !emptyTag(dbTag) {
			setManyFields = append(setManyFields, fmt.Sprintf(`"%s" = "updatedTable"."%s"`, dbTag, dbTag))
		}
	}

	return strings.Join(setManyFields, ",")
}

func idTag(dbTag string) bool {
	return dbTag == "id"
}

func emptyTag(dbTag string) bool {
	emptyTags := []string{"", "-"}
	for _, t := range emptyTags {
		if dbTag == t {
			return true
		}
	}
	return false
}

func readOnlyTag(dbTag string) bool {
	readOnlyTags := []string{"id", "owner", "createdAt", "updatedAt", "deletedAt", "createdBy", "updatedBy", "deletedBy"}
	for _, t := range readOnlyTags {
		if dbTag == t {
			return true
		}
	}
	return false
}
