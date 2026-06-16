with source as (

    select * from {{ source('dlp', 'dim_dlp_cesty') }}

),

renamed as (

    select
        zdrojovy_soubor as source_file,
        try_cast(datum_aktualizace as date) as dataset_updated_at,
        cesta,
        nazev,
        nazev_en,
        nazev_lat,
        try_cast(kod_edqm as integer) as kod_edqm

    from source

)

select * from renamed
