package queue

// func TestDynamoDBQueue_Push(t *testing.T) {
// 	// Create a new mock DynamoDB client
// 	// ctrl := gomock.NewController(t)
// 	// defer ctrl.Finish()
// 	// mockSvc := mock_dynamodb.NewMockDynamoDBAPI(ctrl)

// 	sess, _ := session.NewSessionWithOptions(session.Options{
// 		Config: aws.Config{
// 			Region:   aws.String("us-east-1"),
// 			Endpoint: aws.String("http://localhost:8000"),
// 		},
// 	})

// 	svc := dynamodb.New(sess)

// 	logger := logrus.New()
// 	// Create a new queue with the mock client
// 	q, _ := NewDynamoDBQueue("ipfs-dev", "test-table", svc, logger)

// 	err := q.Push(map[string]any{
// 		"test-key": "test-value",
// 	})
// 	if err != nil {
// 		t.Error(err)
// 	}

// }

// func TestDynamoDBQueue_Pop(t *testing.T) {
// 	// Create a new mock DynamoDB client
// 	// ctrl := gomock.NewController(t)
// 	// defer ctrl.Finish()
// 	// mockSvc := mock_dynamodb.NewMockDynamoDBAPI(ctrl)

// 	sess, _ := session.NewSessionWithOptions(session.Options{
// 		Config: aws.Config{
// 			Region:   aws.String("us-east-1"),
// 			Endpoint: aws.String("http://localhost:8000"),
// 		},
// 	})

// 	svc := dynamodb.New(sess)

// 	logger := logrus.New()
// 	// Create a new queue with the mock client
// 	q, _ := NewDynamoDBQueue("ipfs-dev", "test-table", svc, logger)

// 	item, err := q.Pop()
// 	if err != nil {
// 		t.Error(err)
// 	}

// 	if item != nil {
// 		fmt.Println(*item)
// 	}

// }
