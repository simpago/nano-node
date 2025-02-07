use crate::cli::CliInfrastructure;

pub(crate) fn create_key_pair(infra: &mut CliInfrastructure) {
    let key = infra.key_factory.create_key();

    infra.console.println(format!("Private: {}", key.raw_key()));
    infra
        .console
        .println(format!("Public: {}", key.public_key()));
    infra
        .console
        .println(format!("Account: {}", key.account().encode_account()));
}

#[cfg(test)]
mod tests {
    use crate::cli::{Cli, CliInfrastructure};
    use clap::Parser;

    #[test]
    fn create_key_pair() {
        let cli = Cli::try_parse_from(["nulled_node", "utils", "create-key-pair"]).unwrap();
        let mut infra = CliInfrastructure::new_null();
        let print_tracker = infra.console.track();

        cli.run(&mut infra).unwrap();

        let output = print_tracker.output();
        assert_eq!(
            output,
            [
                "Private: 000000000000002A000000000000002A000000000000002A000000000000002A",
                "Public: 49074D77DBE728CEB5EA2628A75DC7CE21493FDDCFCA991AAA1629F11D99FFD9",
                "Account: nano_1ka9bouxqssasttynbjanxgwhmj3b6zxumycm6fcn7jby6gsmzysauneamau",
            ]
        );
    }
}
