package factory

import (
	"github.com/jinzhu/gorm"
	"github.com/pe-gomes/codepix-go/application/usecases"
	"github.com/pe-gomes/codepix-go/infra/repository"
)

func TransactionUseCaseFactory(database *gorm.DB) usecases.TransactionUseCase {
	pixRepository := repository.PixKeyRepositoryDb{Db: database}
	transactionRepository := repository.TransactionRepositoryDb{Db: database}

	transactionUseCase := usecases.TransactionUseCase{
		TransactionRepository: transactionRepository,
		PixKeyRepository:      pixRepository,
	}

	return transactionUseCase
}
