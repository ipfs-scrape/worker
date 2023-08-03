package queue

import (
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

// IsUnlockedCondition is a global property that represents the condition for getting the next unlocked queue item.
func IsUnlockedCondition() expression.ConditionBuilder {
	return expression.Or(
		expression.Equal(expression.Name("Locked"), expression.Value(false)),
		expression.And(
			expression.Equal(expression.Name("Locked"), expression.Value(true)),
			expression.Or(
				expression.LessThan(expression.Name("LockTime"), expression.Value(time.Now().Add(-time.Hour).UnixNano())),
				expression.AttributeNotExists(expression.Name("LockTime")),
			),
		),
	)
}
