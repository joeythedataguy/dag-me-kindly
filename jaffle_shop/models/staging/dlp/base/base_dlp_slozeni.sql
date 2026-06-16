with source as (

    select * from {{ source('dlp', 'dim_dlp_slozeni') }}

),

renamed as (

    select
        zdrojovy_soubor as source_file,
        try_cast(datum_aktualizace as date) as dataset_updated_at,
        kod_sukl,
        try_cast(kod_latky as integer) as kod_latky,
        try_cast(sq as integer) as sq,
        s,
        amnt_od,
        amnt,
        un

    from source

)

select * from renamed
