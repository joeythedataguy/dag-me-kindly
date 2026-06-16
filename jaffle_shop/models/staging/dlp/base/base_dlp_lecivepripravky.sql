with source as (

    select * from {{ source('dlp', 'dim_dlp_lecivepripravky') }}

),

renamed as (

    select
        zdrojovy_soubor as source_file,
        try_cast(datum_aktualizace as date) as dataset_updated_at,
        kod_sukl,
        h,
        nazev,
        sila,
        forma,
        baleni,
        cesta,
        doplnek,
        obal,
        drz,
        zemdrz,
        akt_drz,
        akt_zem,
        reg,
        try_strptime(v_platdo, '%d%m%y')::date as v_platdo,
        neomez = 'X' as neomez,
        try_strptime(uvadenido, '%d%m%y')::date as uvadenido,
        isx as indikacni_skupina,
        atc_who,
        rc,
        sdov,
        sdov_dod,
        sdov_zem,
        reg_proc,
        try_cast(replace(dddamnt_who, ',', '.') as decimal(9, 3)) as dddamnt_who,
        dddun_who,
        try_cast(replace(dddp_who, ',', '.') as decimal(15, 4)) as dddp_who,
        zdroj_who,
        ll,
        vydej,
        zav,
        doping,
        narvla,
        dodavky,
        ean,
        braillovo_pismo,
        exp,
        exp_t,
        nazev_reg,
        mrp_cislo,
        pravni_zaklad_registrace,
        ochranny_prvek = 'A' as ochranny_prvek,
        omezeni_preskripce_smp = 'A' as omezeni_preskripce_smp,
        typ_lp

    from source

)

select * from renamed
