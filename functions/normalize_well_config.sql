case upper(trim(config_value))
    -- Horizontal variants
    when 'HORIZONTAL' then 'HORIZONTAL'
    when 'HORIZONTAL RE-ENTRY' then 'HORIZONTAL'
    when 'MULTI-LATERAL' then 'HORIZONTAL'
    when 'MULTIPLE LATERALS' then 'HORIZONTAL'
    when 'PILOT & LATERAL' then 'HORIZONTAL'
    when 'PILOT & HORIZONTAL' then 'HORIZONTAL'
    when 'PILOT HOLE AND LATERAL' then 'HORIZONTAL'

    -- Vertical
    when 'VERTICAL' then 'VERTICAL'

    -- Directional variants
    when 'DIRECTIONAL' then 'DIRECTIONAL'
    when 'DEVIATED' then 'DIRECTIONAL'
    when 'DR' then 'DIRECTIONAL'

    -- Unknown / data quality issues
    when 'UNDETERMINED' then 'UNKNOWN'
    when 'SOUTH' then 'UNKNOWN'
    when 'UNKNOWN' then 'UNKNOWN'

    else coalesce(upper(trim(config_value)), 'UNKNOWN')
end
