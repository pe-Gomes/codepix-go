package repository

import (
	"fmt"

	"github.com/jinzhu/gorm"
	"github.com/pe-gomes/codepix-go/domain/model"
)

type PixKeyRepositoryDb struct {
	Db *gorm.DB
}

func (rep PixKeyRepositoryDb) AddBank(bank *model.Bank) error {
	err := rep.Db.Create(bank).Error

	if err != nil {
		return err
	} else {
		return nil
	}
}

func (rep PixKeyRepositoryDb) AddAccount(account *model.Account) error {
	err := rep.Db.Create(account).Error

	if err != nil {
		return err
	} else {
		return nil
	}
}

func (rep PixKeyRepositoryDb) RegisterKey(pixKey *model.PixKey) (*model.PixKey, error) {
	err := rep.Db.Create(pixKey).Error

	if err != nil {
		return nil, err
	}
	return pixKey, err
}

func (rep PixKeyRepositoryDb) FindKeyByKind(kind string, key string) (*model.PixKey, error) {
	var pixKey model.PixKey

	rep.Db.Preload("Account.Bank").First(&pixKey, "kind = ? and key = ?", kind, key)

	if pixKey.ID == "" {
		return nil, fmt.Errorf("no key was found")
	}

	return &pixKey, nil
}

func (rep PixKeyRepositoryDb) FindAccount(id string) (*model.Account, error) {
	var account model.Account

	rep.Db.Find(&account, "id = ?", id)

	if account.ID == "" {
		return nil, fmt.Errorf("no account found")
	}

	return &account, nil
}

func (rep PixKeyRepositoryDb) FindBank(id string) (*model.Bank, error) {
	var bank model.Bank

	rep.Db.Find(&bank, "id = ?", id)

	if bank.ID == "" {
		return nil, fmt.Errorf("no bank found")
	}

	return &bank, nil
}
