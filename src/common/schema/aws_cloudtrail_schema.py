import polars as pl



cloudtrail_schema = {
    "eventVersion": pl.String,
    "eventTime": pl.Datetime,  # ISO 8601 string, can parse to Datetime later
    "userIdentity": pl.Struct({
        # https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-event-reference-user-identity.html
        "type": pl.String,
        "userName": pl.String,
        "principalId": pl.String,
        "arn": pl.String,
        "accountId": pl.String,
        "accessKeyId": pl.String,
        "sessionContext": pl.Struct({
            "sessionIssuer": pl.Struct({
                "type": pl.String,
                "userName": pl.String,
                "principalId": pl.String,
                "arn": pl.String,
                "accountId": pl.String,
            }),
            "webIdFederationData": pl.Struct({
                "federatedProvider": pl.String,
                "attributes": pl.String,
            }),
            "assumedRoot": pl.Boolean,
            "attributes": pl.Struct({
                "creationDate": pl.String,
                "mfaAuthenticated": pl.String,
            }),
            "sourceIdentity": pl.String,
            "ec2RoleDelivery": pl.String,
        }),
        "invokedBy": pl.String,
        "invokedByDelegate": pl.Struct({
                "accountId": pl.String,
            }),
        "onBehalfOf": pl.Struct({
                "userId": pl.String,
                "identityStoreArn": pl.String,
            }),
        "inScopeOf": pl.Struct({
                "sourceArn": pl.String,
                "sourceAccount": pl.String,
            "issuerType": pl.String,
            "credentialsIssuedTo": pl.String,
        }),
        "credentialId": pl.String,
    }),
    "eventSource": pl.String,
    "eventName": pl.Utf8,
    "awsRegion": pl.Utf8,
    "sourceIPAddress": pl.Utf8,
    "userAgent": pl.Utf8,
    "errorCode": pl.Utf8,
    "errorMessage": pl.Utf8,
    "requestParameters": pl.Utf8,  # JSON string
    "responseElements": pl.Utf8,   # JSON string
    "additionalEventData": pl.Utf8,  # JSON string
    "requestID": pl.Utf8,
    "eventID": pl.Utf8,
    "eventType": pl.Utf8,
    "apiVersion": pl.Utf8,
    "managementEvent": pl.Boolean,
    "readOnly": pl.Boolean,
    "resources": pl.List(pl.Struct({
        "ARN": pl.String,
        "accountId": pl.String,
        "type": pl.String,
    })),
    "recipientAccountId": pl.String,
    "serviceEventDetails": pl.String,  # JSON string
    "sharedEventID": pl.String,
    "vpcEndpointId": pl.String,
    "vpcEndpointAccountId": pl.String,
    "eventCategory": pl.String,
    "addendum": pl.Struct({
        "reason": pl.Utf8,
        "updatedFields": pl.Utf8,
        "originalRequestID": pl.Utf8,
        "originalEventID": pl.Utf8,
    }),
    "sessionCredentialFromConsole": pl.Utf8,
    "eventContext": pl.Struct({
        "requestContext": pl.String,
        "tagContext": pl.String,
    }),
    "edgeDeviceDetails": pl.String,  # JSON string
    "tlsDetails": pl.Struct({
        "tlsVersion": pl.String,
        "cipherSuite": pl.String,
        "clientProvidedHostHeader": pl.String,
        "keyExchange": pl.String,
    }),
}
