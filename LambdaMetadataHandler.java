package com.ptg.custom.connector.handler;

import com.amazonaws.appflow.custom.connector.handlers.MetadataHandler;
import com.amazonaws.appflow.custom.connector.model.metadata.*;

import java.util.ArrayList;
import java.util.List;

public class LambdaMetadataHandler implements MetadataHandler {
    @Override
    public ListEntitiesResponse listEntities(ListEntitiesRequest request) {
        final List<Entity> records = new ArrayList<Entity>();
        ListEntitiesResponse response = null;
        try{
            System.out.println("Method listEntities invoked from class LambdaMetadataHandler");
            records.add(
                    ImmutableEntity.builder()
                            .entityIdentifier("JsonToS3")
                            .description("JsonToS3")
                            .label("JsonToS3")
                            .hasNestedEntities(false)
                            .build());

            response =  ImmutableListEntitiesResponse.builder()
                    .addAllEntities(records)
                    .isSuccess(true)
                    .build();
        }catch(Exception e){
            e.printStackTrace();
        }
        return response;
    }

    @Override
    public DescribeEntityResponse describeEntity(DescribeEntityRequest request) {
        System.out.println("Method describeEntity invoked from class LambdaMetadataHandler");
        final List<FieldDefinition> fieldDefinitionsList = new ArrayList<>();

        FieldDefinition idFieldDefinition =  ImmutableFieldDefinition.builder()
                .fieldName("id")
                .dataType(FieldDataType.String)
                .label("id")
                .description("id")
                .defaultValue("313")
                .isPrimaryKey(false)
                .readProperties(ImmutableReadOperationProperty.builder().isQueryable(true)
                        .isRetrievable(true).
                        isTimestampFieldForIncrementalQueries(true)
                        .build())
                .build();

        FieldDefinition nameFieldDefinition =  ImmutableFieldDefinition.builder()
                .fieldName("name")
                .dataType(FieldDataType.String)
                .label("name")
                .description("name")
                .defaultValue("Sagar")
                .isPrimaryKey(false)
                .readProperties(ImmutableReadOperationProperty.builder().isQueryable(true)
                        .isRetrievable(true).
                        isTimestampFieldForIncrementalQueries(true)
                        .build())
                .build();

        FieldDefinition genderFieldDefinition =  ImmutableFieldDefinition.builder()
                .fieldName("gender")
                .dataType(FieldDataType.String)
                .label("gender")
                .description("gender")
                .defaultValue("M")
                .isPrimaryKey(false)
                .readProperties(ImmutableReadOperationProperty.builder().isQueryable(true)
                        .isRetrievable(true).
                        isTimestampFieldForIncrementalQueries(true)
                        .build())
                .build();

        FieldDefinition latitudeFieldDefinition =  ImmutableFieldDefinition.builder()
                .fieldName("latitude")
                .dataType(FieldDataType.String)
                .label("latitude")
                .description("latitude")
                .defaultValue("1.0.0")
                .isPrimaryKey(false)
                .readProperties(ImmutableReadOperationProperty.builder().isQueryable(true)
                        .isRetrievable(true).
                        isTimestampFieldForIncrementalQueries(true)
                        .build())
                .build();

        FieldDefinition longitudeFieldDefinition =  ImmutableFieldDefinition.builder()
                .fieldName("longitude")
                .dataType(FieldDataType.String)
                .label("longitude")
                .description("longitude")
                .defaultValue("2.0.0")
                .isPrimaryKey(false)
                .readProperties(ImmutableReadOperationProperty.builder().isQueryable(true)
                        .isRetrievable(true).
                        isTimestampFieldForIncrementalQueries(true)
                        .build())
                .build();

        //id,name,gender,latitude,longitude

        fieldDefinitionsList.add(idFieldDefinition);
        fieldDefinitionsList.add(nameFieldDefinition);
        fieldDefinitionsList.add(genderFieldDefinition);
        fieldDefinitionsList.add(latitudeFieldDefinition);
        fieldDefinitionsList.add(longitudeFieldDefinition);

        return ImmutableDescribeEntityResponse.builder()
                .isSuccess(true)
                .entityDefinition(
                        ImmutableEntityDefinition.builder()
                                .entity(
                                        ImmutableEntity.builder()
                                                .entityIdentifier(request.entityIdentifier())
                                                .description(request.entityIdentifier())
                                                .label(request.entityIdentifier())
                                                .hasNestedEntities(false)
                                                .build())
                                .fields(fieldDefinitionsList).build())
                .build();
    }
}
