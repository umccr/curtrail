import polars as pl


def with_identity_summary(df: pl.DataFrame) -> pl.DataFrame:
    """Add flat summary columns derived from the nested userIdentity struct.

    Three columns are added:

    identityType  — the userIdentity.type value promoted to a top-level
                     column so callers can group/filter without struct
                     navigation.

    identityActor — a single human-readable identifier for *who* acted:
                       IAMUser    → userName
                       AssumedRole→ session name (the part of principalId
                                    after ":", e.g. "umccr-notify" from
                                    "AROAEXAMPLE:umccr-notify")
                       Root       → "root"
                       Service    → invokedBy
                       AWSService → invokedBy
                       others     → arn, falling back to principalId

    identityRole  — for AssumedRole events, the name of the role that was
                     assumed (sessionContext.sessionIssuer.userName, e.g.
                     "lambda-notify-role"); null for all other identity types.
                     Lets callers group "all actions via role X" independently
                     of who held the session.
    """
    ui = pl.col("userIdentity")
    ui_type = ui.struct.field("type")

    identityType = ui_type.alias("identityType")

    identityActor = (
        pl.when(ui_type == "IAMUser")
        .then(ui.struct.field("userName"))
        .when(ui_type == "AssumedRole")
        .then(ui.struct.field("principalId").str.split(":").list.last())
        .when(ui_type == "Root")
        .then(pl.lit("root"))
        .when(ui_type.is_in(["Service", "AWSService"]))
        .then(ui.struct.field("invokedBy"))
        .otherwise(
            ui.struct.field("arn").fill_null(ui.struct.field("principalId"))
        )
        .alias("identityActor")
    )

    identityRole = (
        pl.when(ui_type == "AssumedRole")
        .then(
            ui.struct.field("sessionContext")
            .struct.field("sessionIssuer")
            .struct.field("userName")
        )
        .otherwise(None)
        .alias("identityRole")
    )

    return df.with_columns([identityType, identityActor, identityRole])