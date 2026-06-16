with source as (

    select * from {{ source('dlp', 'dim_dlp_nazvydokumentu') }}

),

renamed as (

    select
        zdrojovy_soubor as source_file,
        try_cast(datum_aktualizace as date) as dataset_updated_at,
        kod_sukl,
        pil,
        try_strptime(dat_roz_pil, '%d.%m.%Y')::date as dat_roz_pil,
        spc,
        try_strptime(dat_roz_spc, '%d.%m.%Y')::date as dat_roz_spc,
        obal_text,
        try_strptime(dat_roz_obal, '%d.%m.%Y')::date as dat_roz_obal,
        nr,
        try_strptime(dat_npm_nr, '%d.%m.%Y')::date as dat_npm_nr

    from source

)

select * from renamed
