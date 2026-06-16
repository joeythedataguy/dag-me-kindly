with source as (

    select * from {{ source('dlp', 'dim_dlp_zeme') }}

),

renamed as (

    select
        zdrojovy_soubor as source_file,
        try_cast(datum_aktualizace as date) as dataset_updated_at,
        zem,
        nazev,
        nazev_en,
        kod_edqm

    from source

)

select * from renamed
