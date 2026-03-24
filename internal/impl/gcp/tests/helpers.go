package gcp_test

import (
	"context"
	"fmt"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	_ "github.com/warpstreamlabs/bento/public/components/sql"
)

// CreateInstance creates a new Spanner instance with the given project and instance ID.
// Returns the instance name or an error.
func CreateInstance(ctx context.Context, parentProjectID, instanceID string) (string, error) {
	instanceAdminClient, err := instance.NewInstanceAdminClient(ctx)
	if err != nil {
		return "", err
	}
	defer instanceAdminClient.Close()

	op, err := instanceAdminClient.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     "projects/" + parentProjectID,
		InstanceId: instanceID,
		Instance: &instancepb.Instance{
			Config:          "projects/model/instanceConfigs/regional-us-central1",
			DisplayName:     instanceID,
			ProcessingUnits: 100,
		},
	})
	if err != nil {
		return "", err
	}

	resp, err := op.Wait(ctx)
	if err != nil {
		return "", err
	}

	return resp.Name, nil
}

// CreateDatabase creates a new Spanner database with the given parent instance name and database ID.
// Returns the database name or an error.
func CreateDatabase(ctx context.Context, parentInstanceName, databaseID string) (string, error) {
	databaseAdminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return "", err
	}
	defer databaseAdminClient.Close()

	op, err := databaseAdminClient.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          parentInstanceName,
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", databaseID),
	})
	if err != nil {
		return "", err
	}

	resp, err := op.Wait(ctx)
	if err != nil {
		return "", err
	}

	return resp.Name, nil
}
