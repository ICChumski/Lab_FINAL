from pathlib import Path
import great_expectations as gx


GE_DIR = Path("great_expectations").resolve()
CHECKPOINT_NAME = "raw_cycle_trips_checkpoint"


def main():
    print(f"Usando projeto GE em: {GE_DIR}")

    context = gx.get_context(context_root_dir=str(GE_DIR))

    print(f"Executando checkpoint: {CHECKPOINT_NAME}")
    result = context.run_checkpoint(checkpoint_name=CHECKPOINT_NAME)

    print("\nResultado do checkpoint:")
    print(result)

    if not result["success"]:
        raise RuntimeError("Falha ao executar o checkpoint do Great Expectations.")

    print("\nCheckpoint executado com sucesso.")


if __name__ == "__main__":
    main()