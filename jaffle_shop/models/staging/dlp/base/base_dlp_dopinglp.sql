with source as (

    select * from {{ source('dlp', 'dim_dlp_dopinglp') }}

),

renamed as (

    select
        zdrojovy_soubor as source_file,
        try_cast(datum_aktualizace as date) as dataset_updated_at,
        kod_sukl,
        kod_doping

    from source

)

select * from renamed
