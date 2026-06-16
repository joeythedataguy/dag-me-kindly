with source as (

    select * from {{ source('dlp', 'dim_dlp_obaly') }}

),

renamed as (

    select
        zdrojovy_soubor as source_file,
        try_cast(datum_aktualizace as date) as dataset_updated_at,
        obal,
        nazev,
        nazev_en,
        try_cast(kod_edqm as integer) as kod_edqm

    from source

)

select * from renamed
