def wrap_valid_df_with_name(df, target_sector_alias):
    """
    Create a tuple containing a dataframe of valid skills and its display name.
    
    Args:
        df: DataFrame containing valid skills
        target_sector_alias (str): The sector alias to include in the display name
        
    Returns:
        tuple: A tuple containing (dataframe, display_name)
    """
    name = f"Valid Skills for {target_sector_alias} sector"
    return (df, name)


def wrap_invalid_df_with_name(df, target_sector_alias):
    """
    Create a tuple containing a dataframe of invalid skills and its display name.
    
    Args:
        df: DataFrame containing invalid skills
        target_sector_alias (str): The sector alias to include in the display name
        
    Returns:
        tuple: A tuple containing (dataframe, display_name)
    """
    name = f"Invalid Skills for {target_sector_alias}"
    return (df, name)


def wrap_all_df_with_name(df, target_sector_alias):
    """
    Create a tuple containing a dataframe of all tagged skills and its display name.
    
    Args:
        df: DataFrame containing all tagged skills
        target_sector_alias (str): The sector alias to include in the display name
        
    Returns:
        tuple: A tuple containing (dataframe, display_name)
    """
    name = f"All Tagged Skills for {target_sector_alias} sector"
    return (df, name)
