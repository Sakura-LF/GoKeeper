package main

import (
	"GoKeeper"
	"errors"
	"github.com/bytedance/sonic"
	"github.com/gofiber/fiber/v3"
	"log"
	"os"
	"path/filepath"
)

type Response struct {
	Code   int         `json:"code"`
	Data   interface{} `json:"data"`
	Reason string      `json:"reason"`
	Msg    string      `json:"msg"`
}

type DBService struct {
	DB  *GoKeeper.DB
	App *fiber.App
}

func main() {
	// 1.初始化 DB 实例
	options := GoKeeper.DefaultOptions
	options.DirPath = filepath.Join(os.TempDir(), "goKeeper")
	db, err := GoKeeper.Open(options)
	defer func(db *GoKeeper.DB) {
		err := db.Close()
		if err != nil {
			log.Println(err)
			return
		}
	}(db)
	if err != nil {
		panic(err)
	}

	// 2.创建一个 fiber 实例
	app := fiber.New(fiber.Config{
		JSONEncoder: sonic.Marshal,
		JSONDecoder: sonic.Unmarshal,
		AppName:     "GoKeeper",
	})

	// 3.创建一个 DBService 实例
	dbService := DBService{
		DB:  db,
		App: app,
	}

	// api
	dbService.App.Put("/api/v1/goKeeper/kv", dbService.handlerPut)
	dbService.App.Get("/api/v1/goKeeper/kv", dbService.handlerGet)
	dbService.App.Delete("/api/v1/goKeeper/kv", dbService.handlerDelete)
	dbService.App.Get("/api/v1/goKeeper/listKey", dbService.handlerListKeys)
	dbService.App.Get("/api/v1/goKeeper/stat", dbService.handlerStat)

	if err = dbService.App.Listen(":8080", fiber.ListenConfig{
		EnablePrefork:     false,
		EnablePrintRoutes: true,
	}); err != nil {
		return
	}
}

func (dbService *DBService) handlerListKeys(c fiber.Ctx) error {
	response := &Response{
		Code: 200,
	}
	keys := dbService.DB.ListKeys()
	keysStr := make([]string, 0, 500)
	for _, key := range keys {
		keysStr = append(keysStr, string(key))
	}
	response.Data = keysStr
	response.Msg = "list key success"

	return c.JSON(response)
}

func (dbService *DBService) handlerDelete(c fiber.Ctx) error {
	// 构建 Response
	response := &Response{
		Code: 200,
	}
	key := c.Query("key")

	err := dbService.DB.Delete([]byte(key))
	if errors.Is(err, GoKeeper.ErrKeyIsEmpty) {
		c.Status(fiber.StatusBadRequest)
		response.Msg = "key is empty"
		response.Reason = err.Error()
		response.Code = fiber.StatusBadRequest
		return c.JSON(response)
	}
	if errors.Is(err, GoKeeper.ErrKeyNotFound) {
		c.Status(fiber.StatusBadRequest)
		response.Msg = "key not found"
		response.Reason = err.Error()
		return c.JSON(response)
	}
	if err != nil {
		c.Status(fiber.StatusInternalServerError)
		response.Msg = "delete failed"
		response.Reason = err.Error()
		return c.JSON(response)
	}
	response.Msg = "delete success"
	return c.JSON(response)
}

func (dbService *DBService) handlerGet(c fiber.Ctx) error {
	// 构建 Response
	response := &Response{
		Code: 200,
	}
	// 获取请求体
	key := c.Query("key")
	value, err := dbService.DB.Get([]byte(key))
	response.Data = string(value)

	if errors.Is(err, GoKeeper.ErrKeyNotFound) {
		response.Msg = "key not found"
		response.Code = fiber.StatusNotFound
		response.Reason = err.Error()
		c.Status(fiber.StatusNotFound)
		return c.JSON(response)
	}
	if errors.Is(err, GoKeeper.ErrKeyIsEmpty) {
		response.Msg = "key is empty"
		response.Code = fiber.StatusBadRequest
		response.Reason = err.Error()
		c.Status(fiber.StatusBadRequest)
		return c.JSON(response)
	}
	if err != nil {
		response.Msg = "failed to get value in db"
		response.Code = fiber.StatusInternalServerError
		response.Data = nil
		response.Reason = err.Error()
	}

	return c.JSON(response)
}

func (dbService *DBService) handlerPut(c fiber.Ctx) error {
	// 构建 Response
	response := &Response{
		Code: 200,
	}
	data := make(map[string]string)

	// 解析请求体到 map
	if err := sonic.Unmarshal(c.Request().Body(), &data); err != nil {
		c.Status(fiber.StatusBadRequest)
		response.Msg = "parse request body failed"
		response.Reason = err.Error()
		return c.JSON(response)
	}

	for key, val := range data {
		log.Println(key, val)
		if err := dbService.DB.Put([]byte(key), []byte(val)); err != nil {
			c.Status(fiber.StatusInternalServerError)
			response.Msg = "put failed"
			response.Code = fiber.StatusInternalServerError
			response.Reason = err.Error()
			return c.JSON(response)
		}
	}
	response.Msg = "put success"
	return c.JSON(response)
}

func (dbService *DBService) handlerStat(c fiber.Ctx) error {
	response := &Response{
		Code: 200,
	}
	stat := dbService.DB.Stat()
	response.Data = stat
	response.Msg = "get stat success"
	return c.JSON(response)
}
