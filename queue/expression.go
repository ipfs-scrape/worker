package queue

import (
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

// IsUnlockedCondition is a global property that represents the condition for getting the next unlocked queue item.
var IsUnlockedCondition = expression.Or(
	expression.Equal(expression.Name("Locked"), expression.Value(false)),
	expression.And(
		expression.Equal(expression.Name("Locked"), expression.Value(true)),
		expression.Or(
			expression.Not(expression.GreaterThan(expression.Name("LockTime"), expression.Value(time.Now().Add(-time.Second).UnixNano()))),
			expression.AttributeNotExists(expression.Name("LockTime")),
		),
	),
)
