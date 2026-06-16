with source as (

    select * from {{ source('dlp', 'dim_dlp_slozenipriznak') }}

),

renamed as (

    select
        zdrojovy_soubor as source_file,
        try_cast(datum_aktualizace as date) as dataset_updated_at,
        s,
        vyznam

    from source

)

select * from renamed
