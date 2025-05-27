def wrap_valid_df_with_name(df, target_sector_alias):
    name = f"Valid Skills for {target_sector_alias} sector"
    return (df, name)


def wrap_invalid_df_with_name(df, target_sector_alias):
    name = f"Invalid Skills for {target_sector_alias}"
    return (df, name)


def wrap_all_df_with_name(df, target_sector_alias):
    name = f"All Tagged Skills for {target_sector_alias} sector"
    return (df, name)
