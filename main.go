package gorm

import (
	"bytes"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
)

type Organizaion struct {
	ID       string    `gorm:"type:VARCHAR(100);not null"`
	Name     string    `gorm:"type:VARCHAR(100);unique_index:idx_name_name2"`
	Name2    string    `gorm:"type:VARCHAR(100)";unique_index:idx_name_name2`
	CreateAt time.Time `gorm:"type:VARCHAR(100);index"`
}

func main() {
	db, _ := gorm.Open("mysql", "root:123456@/test?charset=utf8&parseTime=True&loc=Local")
	defer db.Close()

	db.LogMode(true)
	db.AutoMigrate(&Organizaion{})
	db.Model(&Organizaion{}).AddForeignKey("name", "companies(name)", "CASCADE", "CASCADE")
}

// =============================== JOIN 查询 ===============================
func joinSelect(db *gorm.DB) {
	orgs := []Organizaion{}
	err := db.Table("organizations o").Select("o.*").
		Joins("left join companies c on c.entity_id = o.id and c.deleted_at is null").
		Where("o.deleted_at is null").Scan(&orgs).Error
	if err != nil && !gorm.IsRecordNotFoundError(err) {
		return
	}
	log.Println(orgs)
}

// =============================== ORDER 查询 ===============================
func orderSelect(db *gorm.DB) {
	orgs := []Organizaion{}
	db.Table("organizaions").Order("id desc, created_at asc").Scan(&orgs)
	log.Println(orgs)
}

// =============================== OFFSET LIMIT 查询 ===============================
func offsetLimitSelect(db *gorm.DB, offset int, limit int) {
	orgs := []Organizaion{}
	// offset : 要跳过的记录数
	db.Table("organizaions").Offset(offset).Limit(limit).Scan(&orgs)
	log.Println(orgs)
}

func countSelect(db *gorm.DB) {
	var count int
	db.Table("organizaions").Count(&count)
	log.Println(count)
}

// =============================== PLUCK 查询 ===============================
func asSelect(db *gorm.DB) {
	orgNames := []string{}
	db.Table("organizaions o").Pluck("o.name", &orgNames)
	log.Println(orgNames)
}

// =============================== GROUP 查询 ===============================
func groupSelect(db *gorm.DB) {
	var tmp []struct {
		Name string `gorm:"type:VARCAHR(100)"`
		ID   string `gorm:"type:VARCHAR(100)"`
	}
	db.Table("organizaions").Select("name, MAX(id)").Group("name").Scan(&tmp)
	log.Println(tmp)
}

// =============================== IN 查询 ===============================
func inSelect(db *gorm.DB) {
	orgs := []Organizaion{}
	ids := []string{"1", "2", "3"}
	err := db.Table("organizaions").Where("id IN (?) and deleted_at is null", ids).Scan(&orgs).Error
	if err != nil {
		return
	}
	log.Println(orgs)
}

// =============================== OR 连接多个条件查询 ===============================
func orSelect(db *gorm.DB, orgs []Organizaion) {
	orgs2 := []Organizaion{}
	var sb strings.Builder
	for i, v := range orgs {
		sb.WriteString(fmt.Sprintf("(id = '%v' and name = '%v' and create_at = '%v')", v.ID, v.Name, v.CreateAt))
		if i < len(orgs)-1 {
			sb.WriteString(" OR ")
		}
	}
	err := db.Table("organizaions").Where(sb.String()).Scan(&orgs2).Error
	if err != nil {
		return
	}
	log.Println(orgs2)
}

// =============================== CREATE 插入一条记录 ===============================
func create(db *gorm.DB) {
	org := Organizaion{
		ID:   "1",
		Name: "asd",
	}
	db.Model(Organizaion{}).Create(&org)
}

// =============================== UPADTEMAP 更新多个字段 ===============================
func updateMap(db *gorm.DB, userID string) {
	updateMap := make(map[string]interface{})
	updateMap["updated_at"] = time.Now()
	updateMap["name"] = "pengjj"
	db.Model(Organizaion{}).Where("user_id = ?", userID).Updates(updateMap)
}

// =============================== UPSERT 表没记录就插入，有记录就更新 ===============================
func upsert(db *gorm.DB, orgs []Organizaion) {
	sql := ""
	for _, v := range orgs {
		sql += `INSERT INTO organizaions (
			id, name, create_at
		) VALUES `
		sql += fmt.Sprintf("( '%s','%s','%s' ) ON DUPLICATE KEY UPDATE name = name + '%s';",
			v.ID, v.Name, v.CreateAt, "pengjj")
	}
	err := db.Exec(sql).Error
	if err != nil {
		return
	}
}

// =============================== OR 条件批量删除 ===============================
func batchDelete(db *gorm.DB, userID string, orgs []Organizaion) {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("DELETE FORM organizaions WHERE user_id = '%s' AND (", userID))
	for _, v := range orgs {
		buffer.WriteString(fmt.Sprintf("( id = '%s' AND name = '%s' ) OR ", v.ID, v.Name))
	}
	sql := buffer.String()
	sql = strings.TrimRight(sql, "OR")
	sql += ")"
	db.Exec(sql)
}

// =============================== 根据前值更新后值 ===============================
func updateLastByFirst(db *gorm.DB, number int) {
	sql := fmt.Sprintf("UPDATE organizaions SET num = num + '%d'", number)
	db.Exec(sql)
}

// =============================== REPLACE 批量先删除后插入 ===============================
func replace(db *gorm.DB, orgs []Organizaion) {
	var buffer bytes.Buffer
	sql := `REPLACE INTO organizaions (id, name, created_at) VALUES`
	_, err := buffer.WriteString(sql)
	if err != nil {
		return
	}
	for _, v := range orgs {
		buffer.WriteString(fmt.Sprintf(`
		('%s','%s','%s')`, v.ID, v.Name, v.CreateAt))
	}
	sql = buffer.String()
	sql = strings.TrimRight(sql, ",")
	db.Exec(sql)
}

// =============================== 缓冲区批量插入 ===============================
func batchInsertByWriteString(db *gorm.DB, orgs []Organizaion) {
	var sb strings.Builder
	sql := "INSERT INTO organizaions (id, name, create_at) "
	if _, err := sb.WriteString(sql); err != nil {
		return
	}
	for i := 0; i < len(orgs); i++ {
		if i == len(orgs)-1 {
			sb.WriteString(fmt.Sprintf("SELECT '%v','%v','%v';", orgs[i].ID, orgs[i].Name, orgs[i].CreateAt))
		} else {
			sb.WriteString(fmt.Sprintf("SELECT '%v','%v','%v' UNION ALL ", orgs[i].ID, orgs[i].Name, orgs[i].CreateAt))
		}
	}
	err := db.Exec(sb.String()).Error
	if err != nil {
		return
	}
}

// =============================== 存在即忽略，否则插入 ===============================
func ignoreInsert(db *gorm.DB, orgs []Organizaion) {
	sql := "INSERT IGNORE INTO organizaions(id, name, created_at) VALUES "
	for i, v := range orgs {
		if i == len(orgs)-1 {
			sql += fmt.Sprintf(" ('%s','%s','%s'); ", v.ID, v.Name, v.CreateAt)
		} else {
			sql += fmt.Sprintf(" ('%s','%s','%s'), ", v.ID, v.Name, v.CreateAt)
		}
	}
	db.Exec(sql)
}

// =============================== CREATE 批量插入(支持指针插入) ===============================
func batchCreate(db *gorm.DB) {
	orgs := []Organizaion{
		{ID: "1", Name: "gr"},
		{ID: "2", Name: "asd"},
	}
	db.Create(&orgs)
}

// =============================== 事务处理(插入更新删除) ===============================
func tx(db *gorm.DB, orgs []Organizaion) {
	tx := db.Begin()
	for _, v := range orgs {
		err := tx.Create(v).Error
		if err != nil {
			tx.Rollback()
			return
		}
	}

	if err := tx.Commit().Error; err != nil {
		return
	}
}

// =============================== 删除特定条件记录 ===============================
func delete(db *gorm.DB, userID string) {
	db.Delete(Organizaion{}).Where("user_id = ?", userID)

	db.Delete(Organizaion{}).Where("user_id = ?", userID).Unscoped()
}

// =============================== 原生查询SQL ===============================
func raw(db *gorm.DB, id string, name string) *gorm.DB {
	return db.Raw(`SELECT * FROM organizaions WHERE id = ? and name like %?%`, id, name)
}
