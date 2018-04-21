(**************************************************************************)
(*                                                                        *)
(*    Copyright (c) 2014 - 2018.                                          *)
(*    Dynamic Ledger Solutions, Inc. <contact@tezos.com>                  *)
(*                                                                        *)
(*    All rights reserved. No warranty, explicit or implicit, provided.   *)
(*                                                                        *)
(**************************************************************************)

open Alpha_context

type error +=
  | Cannot_parse_operation (* `Branch *)
  | Cant_parse_block_header

let () =
  register_error_kind
    `Branch
    ~id:"operation.cannot_parse"
    ~title:"Cannot parse operation"
    ~description:"The operation is ill-formed \
                  or for another protocol version"
    ~pp:(fun ppf () ->
        Format.fprintf ppf "The operation cannot be parsed")
    Data_encoding.unit
    (function Cannot_parse_operation -> Some () | _ -> None)
    (fun () -> Cannot_parse_operation)

let parse_operation (op: Operation.raw) =
  match Data_encoding.Binary.of_bytes
          Operation.protocol_data_encoding
          op.proto with
  | Some protocol_data ->
      ok { shell = op.shell ; protocol_data }
  | None -> error Cannot_parse_operation

let path = RPC_path.(open_root / "context" / "helpers")

module Scripts = struct

  module S = struct

    open Data_encoding

    let path = RPC_path.(path / "scripts")

    let run_code_input_encoding =
      (obj5
         (req "script" Script.expr_encoding)
         (req "storage" Script.expr_encoding)
         (req "input" Script.expr_encoding)
         (req "amount" Tez.encoding)
         (req "contract" Contract.encoding))

    let run_code =
      RPC_service.post_service
        ~description: "Run a piece of code in the current context"
        ~query: RPC_query.empty
        ~input: run_code_input_encoding
        ~output: (obj3
                    (req "storage" Script.expr_encoding)
                    (req "operations" (list Operation.internal_operation_encoding))
                    (opt "big_map_diff" (list (tup2 string (option Script.expr_encoding)))))
        RPC_path.(path / "run_code")

    let trace_code =
      RPC_service.post_service
        ~description: "Run a piece of code in the current context, \
                       keeping a trace"
        ~query: RPC_query.empty
        ~input: run_code_input_encoding
        ~output: (obj4
                    (req "storage" Script.expr_encoding)
                    (req "operations" (list Operation.internal_operation_encoding))
                    (req "trace"
                       (list @@ obj3
                          (req "location" Script.location_encoding)
                          (req "gas" Gas.encoding)
                          (req "stack" (list (Script.expr_encoding)))))
                    (opt "big_map_diff" (list (tup2 string (option Script.expr_encoding)))))
        RPC_path.(path / "trace_code")

    let typecheck_code =
      RPC_service.post_service
        ~description: "Typecheck a piece of code in the current context"
        ~query: RPC_query.empty
        ~input: (obj2
                   (req "program" Script.expr_encoding)
                   (opt "gas" z))
        ~output: (obj2
                    (req "type_map" Script_tc_errors_registration.type_map_enc)
                    (req "gas" Gas.encoding))
        RPC_path.(path / "typecheck_code")

    let typecheck_data =
      RPC_service.post_service
        ~description: "Check that some data expression is well formed \
                       and of a given type in the current context"
        ~query: RPC_query.empty
        ~input: (obj3
                   (req "data" Script.expr_encoding)
                   (req "type" Script.expr_encoding)
                   (opt "gas" z))
        ~output: (obj1 (req "gas" Gas.encoding))
        RPC_path.(path / "typecheck_data")

    let hash_data =
      RPC_service.post_service
        ~description: "Computes the hash of some data expression \
                       using the same algorithm as script instruction H"

        ~input: (obj3
                   (req "data" Script.expr_encoding)
                   (req "type" Script.expr_encoding)
                   (opt "gas" z))
        ~output: (obj2
                    (req "hash" string)
                    (req "gas" Gas.encoding))
        ~query: RPC_query.empty
        RPC_path.(path / "hash_data")

  end

  let () =
    let open Services_registration in
    register0 S.run_code begin fun ctxt ()
      (code, storage, parameter, amount, contract) ->
      Lwt.return (Gas.set_limit ctxt (Constants.hard_gas_limit_per_operation ctxt)) >>=? fun ctxt ->
      let ctxt = Contract.init_origination_nonce ctxt Operation_hash.zero in
      let storage = Script.lazy_expr storage in
      let code = Script.lazy_expr code in
      Script_interpreter.execute
        ctxt Readable
        ~source:contract (* transaction initiator *)
        ~payer:contract (* storage fees payer *)
        ~self:(contract, { storage ; code }) (* script owner *)
        ~amount ~parameter
      >>=? fun { Script_interpreter.storage ; operations ; big_map_diff ; _ } ->
      return (storage, operations, big_map_diff)
    end ;
    register0 S.trace_code begin fun ctxt ()
      (code, storage, parameter, amount, contract) ->
      Lwt.return (Gas.set_limit ctxt (Constants.hard_gas_limit_per_operation ctxt)) >>=? fun ctxt ->
      let ctxt = Contract.init_origination_nonce ctxt Operation_hash.zero in
      let storage = Script.lazy_expr storage in
      let code = Script.lazy_expr code in
      Script_interpreter.trace
        ctxt Readable
        ~source:contract (* transaction initiator *)
        ~payer:contract (* storage fees payer *)
        ~self:(contract, { storage ; code }) (* script owner *)
        ~amount ~parameter
      >>=? fun ({ Script_interpreter.storage ; operations ; big_map_diff ; _ }, trace) ->
      return (storage, operations, trace, big_map_diff)
    end ;
    register0 S.typecheck_code begin fun ctxt () (expr, maybe_gas) ->
      begin match maybe_gas with
        | None -> return (Gas.set_unlimited ctxt)
        | Some gas -> Lwt.return (Gas.set_limit ctxt gas) end >>=? fun ctxt ->
      Script_ir_translator.typecheck_code ctxt expr >>=? fun (res, ctxt) ->
      return (res, Gas.level ctxt)
    end ;
    register0 S.typecheck_data begin fun ctxt () (data, ty, maybe_gas) ->
      begin match maybe_gas with
        | None -> return (Gas.set_unlimited ctxt)
        | Some gas -> Lwt.return (Gas.set_limit ctxt gas) end >>=? fun ctxt ->
      Script_ir_translator.typecheck_data ctxt (data, ty) >>=? fun ctxt ->
      return (Gas.level ctxt)
    end ;
    register0 S.hash_data begin fun ctxt () (expr, typ, maybe_gas) ->
      let open Script_ir_translator in
      begin match maybe_gas with
        | None -> return (Gas.set_unlimited ctxt)
        | Some gas -> Lwt.return (Gas.set_limit ctxt gas) end >>=? fun ctxt ->
      Lwt.return (parse_ty ~allow_big_map:false ~allow_operation:false (Micheline.root typ)) >>=? fun (Ex_ty typ, _) ->
      parse_data ctxt typ (Micheline.root expr) >>=? fun (data, ctxt) ->
      Script_ir_translator.hash_data ctxt typ data >>=? fun (hash, ctxt) ->
      return (hash, Gas.level ctxt)
    end

  let run_code ctxt block code (storage, input, amount, contract) =
    RPC_context.make_call0 S.run_code ctxt
      block () (code, storage, input, amount, contract)

  let trace_code ctxt block code (storage, input, amount, contract) =
    RPC_context.make_call0 S.trace_code ctxt
      block () (code, storage, input, amount, contract)

  let typecheck_code ctxt block =
    RPC_context.make_call0 S.typecheck_code ctxt block ()

  let typecheck_data ctxt block =
    RPC_context.make_call0 S.typecheck_data ctxt block ()

  let hash_data ctxt block =
    RPC_context.make_call0 S.hash_data ctxt block ()

end

module Forge = struct

  module S = struct

    open Data_encoding

    let path = RPC_path.(path / "forge")

    let operations =
      RPC_service.post_service
        ~description:"Forge an operation"
        ~query: RPC_query.empty
        ~input: Operation.unsigned_encoding
        ~output: bytes
        RPC_path.(path / "operations" )

    let empty_proof_of_work_nonce =
      MBytes.of_string
        (String.make Constants_repr.proof_of_work_nonce_size  '\000')

    let protocol_data =
      RPC_service.post_service
        ~description: "Forge the protocol-specific part of a block header"
        ~query: RPC_query.empty
        ~input:
          (obj3
             (req "priority" uint16)
             (opt "nonce_hash" Nonce_hash.encoding)
             (dft "proof_of_work_nonce"
                (Fixed.bytes
                   Alpha_context.Constants.proof_of_work_nonce_size)
                empty_proof_of_work_nonce))
        ~output: (obj1 (req "protocol_data" bytes))
        RPC_path.(path / "protocol_data")

  end

  let () =
    let open Services_registration in
    register0_noctxt S.operations begin fun () (shell, proto) ->
      return (Data_encoding.Binary.to_bytes_exn
                Operation.unsigned_encoding (shell, proto))
    end ;
    register0_noctxt S.protocol_data begin fun ()
      (priority, seed_nonce_hash, proof_of_work_nonce) ->
      return (Data_encoding.Binary.to_bytes_exn
                Block_header.contents_encoding
                { priority ; seed_nonce_hash ; proof_of_work_nonce })
    end

  module Manager = struct

    let operations ctxt
        block ~branch ~source ?sourcePubKey ~counter ~fee
        ~gas_limit ~storage_limit operations =
      Contract_services.manager_key ctxt block source >>= function
      | Error _ as e -> Lwt.return e
      | Ok (_, revealed) ->
          let operations =
            match revealed with
            | Some _ -> operations
            | None ->
                match sourcePubKey with
                | None -> operations
                | Some pk -> Reveal pk :: operations in
          let ops =
            Manager_operations { source ;
                                 counter ; operations ; fee ;
                                 gas_limit ; storage_limit } in
          (RPC_context.make_call0 S.operations ctxt block
             () ({ branch }, Sourced_operation ops))

    let reveal ctxt
        block ~branch ~source ~sourcePubKey ~counter ~fee ()=
      operations ctxt block ~branch ~source ~sourcePubKey ~counter ~fee
        ~gas_limit:Z.zero ~storage_limit:0L []

    let transaction ctxt
        block ~branch ~source ?sourcePubKey ~counter
        ~amount ~destination ?parameters
        ~gas_limit ~storage_limit ~fee ()=
      let parameters = Option.map ~f:Script.lazy_expr parameters in
      operations ctxt block ~branch ~source ?sourcePubKey ~counter
        ~fee ~gas_limit ~storage_limit
        Alpha_context.[Transaction { amount ; parameters ; destination }]

    let origination ctxt
        block ~branch
        ~source ?sourcePubKey ~counter
        ~managerPubKey ~balance
        ?(spendable = true)
        ?(delegatable = true)
        ?delegatePubKey ?script
        ~gas_limit ~storage_limit ~fee () =
      operations ctxt block ~branch ~source ?sourcePubKey ~counter
        ~fee ~gas_limit ~storage_limit
        Alpha_context.[
          Origination { manager = managerPubKey ;
                        delegate = delegatePubKey ;
                        script ;
                        spendable ;
                        delegatable ;
                        credit = balance ;
                        preorigination = None }
        ]

    let delegation ctxt
        block ~branch ~source ?sourcePubKey ~counter ~fee delegate =
      operations ctxt block ~branch ~source ?sourcePubKey ~counter ~fee
        ~gas_limit:Z.zero ~storage_limit:0L
        Alpha_context.[Delegation delegate]

  end

  module Consensus = struct

    let operations ctxt
        block ~branch operation =
      let ops = Consensus_operation operation in
      (RPC_context.make_call0 S.operations ctxt block
         () ({ branch }, Sourced_operation ops))

    let endorsement ctxt
        b ~branch ~block ~level ~slots () =
      operations ctxt b ~branch
        Alpha_context.(Endorsements { block ; level ; slots })


  end

  module Amendment = struct

    let operation ctxt
        block ~branch ~source operation =
      let ops = Amendment_operation { source ; operation } in
      (RPC_context.make_call0 S.operations ctxt block
         () ({ branch }, Sourced_operation ops))

    let proposals ctxt
        b ~branch ~source ~period ~proposals () =
      operation ctxt b ~branch ~source
        Alpha_context.(Proposals { period ; proposals })

    let ballot ctxt
        b ~branch ~source ~period ~proposal ~ballot () =
      operation ctxt b ~branch ~source
        Alpha_context.(Ballot { period ; proposal ; ballot })

  end

  module Dictator = struct

    let operation ctxt
        block ~branch operation =
      let op = Dictator_operation operation in
      (RPC_context.make_call0 S.operations ctxt block
         () ({ branch }, Sourced_operation op))

    let activate ctxt
        b ~branch hash =
      operation ctxt b ~branch (Activate hash)

    let activate_testchain ctxt
        b ~branch hash =
      operation ctxt b ~branch (Activate_testchain hash)

  end

  module Anonymous = struct

    let operations ctxt block ~branch operations =
      (RPC_context.make_call0 S.operations ctxt block
         () ({ branch }, Anonymous_operations operations))

    let seed_nonce_revelation ctxt
        block ~branch ~level ~nonce () =
      operations ctxt block ~branch [Seed_nonce_revelation { level ; nonce }]

  end

  let empty_proof_of_work_nonce =
    MBytes.of_string
      (String.make Constants_repr.proof_of_work_nonce_size  '\000')

  let protocol_data ctxt
      block
      ~priority ?seed_nonce_hash
      ?(proof_of_work_nonce = empty_proof_of_work_nonce)
      () =
    RPC_context.make_call0 S.protocol_data
      ctxt block () (priority, seed_nonce_hash, proof_of_work_nonce)

end

module Parse = struct

  module S = struct

    open Data_encoding

    let path = RPC_path.(path / "parse")

    let operations =
      RPC_service.post_service
        ~description:"Parse operations"
        ~query: RPC_query.empty
        ~input:
          (obj2
             (req "operations" (list (dynamic_size Operation.raw_encoding)))
             (opt "check_signature" bool))
        ~output: (list (dynamic_size Operation.encoding))
        RPC_path.(path / "operations" )

    let block =
      RPC_service.post_service
        ~description:"Parse a block"
        ~query: RPC_query.empty
        ~input: Block_header.raw_encoding
        ~output: Block_header.protocol_data_encoding
        RPC_path.(path / "block" )

  end

  module I = struct

    let check_signature ctxt signature shell contents =
      match contents with
      | Anonymous_operations _ -> return ()
      | Sourced_operation (Manager_operations op) ->
          let public_key =
            List.fold_left (fun acc op ->
                match op with
                | Reveal pk -> Some pk
                | _ -> acc) None op.operations in
          begin
            match public_key with
            | Some key -> return key
            | None ->
                Contract.get_manager ctxt op.source >>=? fun manager ->
                Roll.delegate_pubkey ctxt manager
          end >>=? fun public_key ->
          Operation.check_signature public_key
            { shell ; protocol_data = { contents ; signature } }
      | Sourced_operation (Consensus_operation (Endorsements { level ; slots ; _ })) ->
          let level = Level.from_raw ctxt level in
          Baking.check_endorsements_rights ctxt level slots >>=? fun public_key ->
          Operation.check_signature public_key
            { shell ; protocol_data = { contents ; signature } }
      | Sourced_operation (Amendment_operation { source ; _ }) ->
          Roll.delegate_pubkey ctxt source >>=? fun source ->
          Operation.check_signature source
            { shell ; protocol_data = { contents ; signature } }
      | Sourced_operation (Dictator_operation _) ->
          let key = Constants.dictator_pubkey ctxt in
          Operation.check_signature key
            { shell ; protocol_data = { contents ; signature } }

  end

  let parse_protocol_data protocol_data =
    match
      Data_encoding.Binary.of_bytes
        Block_header.protocol_data_encoding
        protocol_data
    with
    | None -> failwith "Cant_parse_protocol_data"
    | Some protocol_data -> return protocol_data

  let () =
    let open Services_registration in
    register0 S.operations begin fun ctxt () (operations, check) ->
      map_s begin fun raw ->
        Lwt.return (parse_operation raw) >>=? fun op ->
        begin match check with
          | Some true ->
              I.check_signature ctxt
                op.protocol_data.signature op.shell op.protocol_data.contents
          | Some false | None -> return ()
        end >>|? fun () -> op
      end operations
    end ;
    register0_noctxt S.block begin fun () raw_block ->
      parse_protocol_data raw_block.protocol_data
    end

  let operations ctxt block ?check operations =
    RPC_context.make_call0
      S.operations ctxt block () (operations, check)
  let block ctxt block shell protocol_data =
    RPC_context.make_call0
      S.block ctxt block () ({ shell ; protocol_data } : Block_header.raw)

end

module S = struct

  open Data_encoding

  type level_query = {
    offset: int32 ;
  }
  let level_query : level_query RPC_query.t =
    let open RPC_query in
    query (fun offset -> { offset })
    |+ field "offset" RPC_arg.int32 0l (fun t -> t.offset)
    |> seal

  let level =
    RPC_service.get_service
      ~description: "..."
      ~query: level_query
      ~output: Level.encoding
      RPC_path.(path / "level")

  let levels =
    RPC_service.get_service
      ~description: "Levels of a cycle"
      ~query: level_query
      ~output: (obj2
                  (req "first" Raw_level.encoding)
                  (req "last" Raw_level.encoding))
      RPC_path.(path / "levels_in_current_cycle")

end

let () =
  let open Services_registration in
  register0 S.level begin fun ctxt q () ->
    let level = Level.current ctxt in
    return (Level.from_raw ctxt ~offset:q.offset level.level)
  end ;
  register0 S.levels begin fun ctxt q () ->
    let levels = Level.levels_in_current_cycle ctxt ~offset:q.offset () in
    let first = List.hd (List.rev levels) in
    let last = List.hd levels in
    return (first.level, last.level)
  end

let level ctxt ?(offset = 0l) block =
  RPC_context.make_call0 S.level ctxt block { offset } ()

let levels ctxt block cycle =
  RPC_context.make_call1 S.levels ctxt block cycle () ()
