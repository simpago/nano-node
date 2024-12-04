use rsnano_core::{Account, PublicKey, RawKey};
use test_helpers::{setup_rpc_client_and_server, System};

#[test]
fn key_expand() {
    let mut system = System::new();
    let node = system.make_node();

    let server = setup_rpc_client_and_server(node.clone(), false);

    let result = node.runtime.block_on(async {
        server
            .client
            .key_expand(
                RawKey::decode_hex(
                    "781186FB9EF17DB6E3D1056550D9FAE5D5BBADA6A6BC370E4CBB938B1DC71DA3",
                )
                .unwrap(),
            )
            .await
            .unwrap()
    });

    assert_eq!(
        result.private,
        RawKey::decode_hex("781186FB9EF17DB6E3D1056550D9FAE5D5BBADA6A6BC370E4CBB938B1DC71DA3")
            .unwrap()
    );

    assert_eq!(
        result.public,
        PublicKey::decode_hex("3068BB1CA04525BB0E416C485FE6A67FD52540227D267CC8B6E8DA958A7FA039")
            .unwrap()
    );

    assert_eq!(
        result.account,
        Account::decode_account(
            "nano_1e5aqegc1jb7qe964u4adzmcezyo6o146zb8hm6dft8tkp79za3sxwjym5rx"
        )
        .unwrap()
    );
}
