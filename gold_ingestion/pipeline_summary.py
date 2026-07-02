# ==========================================================
# PIPELINE SUMMARY
# ==========================================================

def pipeline_summary(

    total_tables,

    success_tables,

    failed_tables,

    total_rows

):

    print("\n")

    print("=" * 60)

    print("GOLD PIPELINE SUMMARY")

    print("=" * 60)

    print(f"Tables Processed : {total_tables}")

    print(f"Successful      : {success_tables}")

    print(f"Failed          : {failed_tables}")

    print(f"Rows Written    : {total_rows}")

    print("=" * 60)