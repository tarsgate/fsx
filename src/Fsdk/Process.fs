namespace Fsdk

open System
open System.IO
open System.Diagnostics
open System.Threading
open System.Linq
open System.Text

module Process =

    // https://stackoverflow.com/a/961904/544947
    type internal QueuedLock() =
        let innerLock = Object()
        let mutable ticketsCount = 0
        let mutable ticketToRide = 1

        member __.Enter() =
            let myTicket = Interlocked.Increment &ticketsCount
            Monitor.Enter innerLock

            while myTicket <> Volatile.Read &ticketToRide do
                Monitor.Wait innerLock |> ignore

        member __.Exit() =
            Interlocked.Increment &ticketToRide |> ignore
            Monitor.PulseAll innerLock
            Monitor.Exit innerLock

    type Standard =
        | Output
        | Error

        override self.ToString() =
            sprintf "%A" self

    type OutputChunk =
        {
            OutputType: Standard
            Chunk: StringBuilder
        }

    type Echo =
        | All
        | OutputOnly
        | Off

    type OutputBuffer(buffer: list<OutputChunk>) =

        //NOTE the buffer is built by prepending (see ReadIteration),
        // so we List.rev before iterating to restore chronological order

        // Iterative version: List.rev + fold/iter avoids stack overflow on large buffers
        // (the buffer is built by prepending, so rev restores chronological order)
        let FilterByOutputType
            (
                subBuffer: list<OutputChunk>,
                outputType: Option<Standard>
            ) : StringBuilder =
            let newStringBuilder = StringBuilder()

            subBuffer
            |> List.rev
            |> List.fold
                (fun (sb: StringBuilder) chunk ->
                    if (outputType.IsNone || chunk.OutputType = outputType.Value) then
                        sb.Append(chunk.Chunk.ToString())
                    else
                        sb
                )
                newStringBuilder

        let Print(subBuffer: list<OutputChunk>) : unit =
            subBuffer
            |> List.rev
            |> List.iter(fun chunk ->
                match chunk.OutputType with
                | Standard.Output ->
                    Console.Write(chunk.Chunk.ToString())
                    Console.Out.Flush()
                | Standard.Error ->
                    Console.Error.Write(chunk.Chunk.ToString())
                    Console.Error.Flush()
            )

        member this.StdOut =
            FilterByOutputType(buffer, Some(Standard.Output))
                .ToString()

        member this.StdErr =
            FilterByOutputType(buffer, Some(Standard.Error))
                .ToString()

        member this.PrintToConsole() =
            Print(buffer)

        override self.ToString() =
            FilterByOutputType(buffer, None).ToString()

    type ProcessResultState =
        // exitCode=0, no stdErr
        | Success of output: string

        // exitCode<>0
        | Error of exitCode: int * output: OutputBuffer

        // exitCode=0, some stdErr
        | WarningsOrAmbiguous of output: OutputBuffer

    type ProcessDetails =
        {
            Command: string
            Arguments: string
        }

        override self.ToString() =
            sprintf "Command: %s. Arguments: %s." self.Command self.Arguments


    exception ProcessSucceededWithWarnings of string
    exception ProcessFailed of string

    type RunDetails =
        {
            Command: string
            Args: string
            Echo: Echo
        }

    type ProcessResult =
        {
            Details: RunDetails
            Result: ProcessResultState
        }

        member self.Unwrap(errMsg: string) : string =
            match self.Result with
            | Success output -> output
            | Error(_, output) ->
                if self.Details.Echo = Echo.Off then
                    output.PrintToConsole()
                    Console.WriteLine()
                    Console.Out.Flush()

                Console.Error.WriteLine errMsg
                raise <| ProcessFailed errMsg
            | WarningsOrAmbiguous output ->
                if self.Details.Echo = Echo.Off then
                    output.PrintToConsole()
                    Console.WriteLine()
                    Console.Out.Flush()

                let fullErrMsg = sprintf "%s (with warnings?)" errMsg
                Console.Error.WriteLine fullErrMsg
                raise <| ProcessSucceededWithWarnings fullErrMsg

        member self.UnwrapDefault(?throwWhenWarnings: bool) : string =
            let throwWhenWarnings = defaultArg throwWhenWarnings true

            match self.Result with
            | WarningsOrAmbiguous output when (throwWhenWarnings = false) ->
                output.ToString()
            | _ ->
                self.Unwrap(
                    sprintf
                        "Error when running '%s %s'"
                        self.Details.Command
                        self.Details.Args
                )


    type ProcessCouldNotStart
        (
            procDetails: ProcessDetails,
            innerException: Exception
        ) =
        inherit Exception
            (
                sprintf "Process could not start! %s" (procDetails.ToString()),
                innerException
            )


type Process =

    // TODO: explore if this complex implementation can be replaced with https://www.nuget.org/packages/Owl.cli
    //       (maybe only after https://github.com/tatsuya-midorikawa/Owl.cli/issues/3 is addressed)
    static member Execute
        (
            procDetails: Process.ProcessDetails,
            echo: Process.Echo
        ) : Process.ProcessResult =

        // I know, this shit below is mutable, but it's a consequence of dealing with .NET's Process class' events?
        let mutable outputBuffer: list<Process.OutputChunk> = []
        let queuedLock = Process.QueuedLock()

        if (echo = Process.Echo.All) then
            Console.WriteLine(
                sprintf "%s %s" procDetails.Command procDetails.Arguments
            )

            Console.Out.Flush()

        let startInfo =
            new ProcessStartInfo(procDetails.Command, procDetails.Arguments)

        startInfo.UseShellExecute <- false
        startInfo.RedirectStandardOutput <- true
        startInfo.RedirectStandardError <- true
        use proc = new System.Diagnostics.Process()
        proc.StartInfo <- startInfo

        let ReadStandard(std: Process.Standard) =

            let print(str: string) =
                match std with
                | Process.Standard.Output -> Console.Out.Write str
                | Process.Standard.Error -> Console.Error.Write str

            let flush =
                match std with
                | Process.Standard.Output -> Console.Out.Flush
                | Process.Standard.Error -> Console.Error.Flush

            let outputToReadFrom =
                match std with
                | Process.Standard.Output -> proc.StandardOutput
                | Process.Standard.Error -> proc.StandardError

            // I want to hardcode this to 1 because otherwise the order of the stderr|stdout
            // chunks in the outputbuffer would innecessarily depend on this bufferSize, setting
            // it to 1 makes it slow but then the order is only relying (in theory) on how the
            // streams come and how fast the .NET IO processes them
            let bufferSize = 1
            let readBuffer = Array.zeroCreate<char> bufferSize
            let lineBuffer = StringBuilder()

            let append(chunkString: string) : unit =
                match outputBuffer with
                | [] ->
                    let newBlock =
                        {
                            Process.OutputChunk.OutputType = std
                            Process.OutputChunk.Chunk =
                                StringBuilder chunkString
                        }

                    outputBuffer <- List.singleton newBlock
                | head :: _tail ->
                    if head.OutputType = std then
                        head.Chunk.Append chunkString |> ignore
                    else
                        let newBlock =
                            {
                                Process.OutputChunk.OutputType = std
                                Process.OutputChunk.Chunk =
                                    StringBuilder chunkString
                            }

                        outputBuffer <- newBlock :: outputBuffer

                if not(echo = Process.Echo.Off) then
                    print chunkString
                    flush()

            let flushLineBuffer(emitAll: bool) =
                let str = lineBuffer.ToString()

                if str.Length > 0 then
                    let charsToEmit =
                        if emitAll then
                            lineBuffer.Clear() |> ignore
                            str
                        else
                            let lastEol = str.LastIndexOf('\n')

                            if lastEol >= 0 then
                                let emitPart = str.Substring(0, lastEol + 1)
                                lineBuffer.Remove(0, lastEol + 1) |> ignore
                                emitPart
                            elif str.Length >= 65536 then
                                lineBuffer.Clear() |> ignore
                                str
                            else
                                String.Empty

                    if charsToEmit.Length > 0 then
                        try
                            queuedLock.Enter()
                            append charsToEmit
                        finally
                            queuedLock.Exit()

            let ReadIteration() : bool =
                let readCount = outputToReadFrom.Read(readBuffer, 0, bufferSize)

                if readCount <= 0 then
                    flushLineBuffer true
                    false
                else
                    lineBuffer.Append(readBuffer, 0, readCount) |> ignore
                    flushLineBuffer false
                    true

            // this is a way to do a `do...while` loop in F#...
            while (ReadIteration()) do
                ignore None

        let outReaderThread =
            new Thread(
                new ThreadStart(fun _ -> ReadStandard(Process.Standard.Output))
            )

        let errReaderThread =
            new Thread(
                new ThreadStart(fun _ -> ReadStandard(Process.Standard.Error))
            )

        try
            proc.Start() |> ignore
        with
        | ex -> raise <| Process.ProcessCouldNotStart(procDetails, ex)

        outReaderThread.Start()
        errReaderThread.Start()
        proc.WaitForExit()
        let exitCode = proc.ExitCode

        outReaderThread.Join()
        errReaderThread.Join()

        let output = Process.OutputBuffer outputBuffer

        let procRunResultDetails =
            {
                Process.RunDetails.Command = procDetails.Command
                Process.RunDetails.Args = procDetails.Arguments
                Process.RunDetails.Echo = echo
            }

        match exitCode with
        | 0 when output.StdErr.Length = 0 ->
            {
                Process.ProcessResult.Details = procRunResultDetails
                Process.ProcessResult.Result =
                    Process.ProcessResultState.Success output.StdOut
            }
        | 0 ->
            {
                Process.ProcessResult.Details = procRunResultDetails
                Process.ProcessResult.Result =
                    Process.ProcessResultState.WarningsOrAmbiguous output
            }
        | _ ->
            {
                Process.ProcessResult.Details = procRunResultDetails
                Process.ProcessResult.Result =
                    Process.ProcessResultState.Error(exitCode, output)
            }

#if !LEGACY_FRAMEWORK

    static member ExecDefault
        (
            commandAndArgs: string,
            ?echo: Process.Echo
        ) : Process.ProcessResult =
        let echo = defaultArg echo Process.Echo.All
        let commandAndArgs = commandAndArgs.Trim()

        let rec findUnescapedQuote(startIdx: int) : int =
            let idx = commandAndArgs.IndexOf('"', startIdx)

            if idx < 0 then
                -1
            elif idx > 0 && commandAndArgs.[idx - 1] = '\\' then
                findUnescapedQuote(idx + 1)
            else
                idx

        let command, arguments =
            let commandAndArgs = commandAndArgs.Trim()

            if commandAndArgs.StartsWith("\"") then
                let closeIdx = findUnescapedQuote 1

                if closeIdx < 0 then
                    // Unclosed quote: treat as unquoted
                    let spaceIdx = commandAndArgs.IndexOf(' ')

                    if spaceIdx < 0 then
                        commandAndArgs, String.Empty
                    else
                        commandAndArgs.Substring(0, spaceIdx),
                        commandAndArgs.Substring(spaceIdx + 1).Trim()
                else
                    commandAndArgs.Substring(0, closeIdx + 1),
                    commandAndArgs.Substring(closeIdx + 1).Trim()
            else
                let spaceIdx = commandAndArgs.IndexOf(' ')

                if spaceIdx < 0 then
                    commandAndArgs, String.Empty
                else
                    commandAndArgs.Substring(0, spaceIdx),
                    commandAndArgs.Substring(spaceIdx + 1).Trim()

        Process.Execute(
            {
                Process.ProcessDetails.Command = command
                Process.ProcessDetails.Arguments = arguments
            },
            echo
        )

#endif

    static member private ExceptionIsOfTypeOrIncludesAnyInnerExceptionOfType
        (
            ex: Exception,
            t: Type
        ) : bool =
        if (ex = null) then
            false
        else if (ex.GetType() = t) then
            true
        else
            Process.ExceptionIsOfTypeOrIncludesAnyInnerExceptionOfType(
                ex.InnerException,
                t
            )

    static member private CheckIfCommandWorksInShellWithWhich
        (command: string)
        : bool =
        let WhichCommandWorksInShell() : bool =
            let maybeResult =
                try
                    Some(
                        Process.Execute(
                            {
                                Process.ProcessDetails.Command = "which"
                                Process.ProcessDetails.Arguments = String.Empty
                            },
                            Process.Echo.Off
                        )
                    )
                with
                | ex when
                    (Process.ExceptionIsOfTypeOrIncludesAnyInnerExceptionOfType(
                        ex,
                        typeof<System.ComponentModel.Win32Exception>
                    ))
                    ->
                    None
                | _ -> reraise()

            match maybeResult with
            | None -> false
            | Some _ -> true

        if not(WhichCommandWorksInShell()) then
            failwith "'which' doesn't work, please install it first"

        let proc =
            Process.Execute(
                {
                    Process.ProcessDetails.Command = "which"
                    Process.ProcessDetails.Arguments = command
                },
                Process.Echo.Off
            )

        match proc.Result with
        | Process.ProcessResultState.Error _ -> false
        | Process.ProcessResultState.WarningsOrAmbiguous output ->
            output.PrintToConsole()
            Console.WriteLine()
            Console.Out.Flush()
            Console.Error.Flush()
            failwith "Unexpected 'which' output ^ (with warnings?)"
        | Process.ProcessResultState.Success _ -> true

    static member private HasWindowsExecutableExtension(path: string) =
        //FIXME: should do it in a case-insensitive way
        path.EndsWith(".exe")
        || path.EndsWith(".bat")
        || path.EndsWith(".cmd")
        || path.EndsWith(".com")

    static member private IsFileInWindowsPath(command: string) =
        let pathEnvVar = Environment.GetEnvironmentVariable("PATH")
        let paths = pathEnvVar.Split(Path.PathSeparator)
        paths.Any(fun path -> File.Exists(Path.Combine(path, command)))

    static member CommandWorksInShell(command: string) : bool =
        if (Misc.GuessPlatform() = Misc.Platform.Windows) then
            let exists =
                File.Exists(command) || Process.IsFileInWindowsPath(command)

            if (exists && Process.HasWindowsExecutableExtension(command)) then
                true
            else
                try
                    Process.Execute(
                        {
                            Process.ProcessDetails.Command = command
                            Process.ProcessDetails.Arguments = String.Empty
                        },
                        Process.Echo.Off
                    )
                    |> ignore<Process.ProcessResult>

                    true
                with
                | :? Process.ProcessCouldNotStart -> false
        else
            Process.CheckIfCommandWorksInShellWithWhich(command)

    static member ConfigCommandCheck
        (commandNamesByOrderOfPreference: seq<string>)
        (exitIfNotFound: bool)
        (printConfigureChecks: bool)
        : Option<string> =
        let rec configCommandCheck currentCommandNamesQueue allCommands =
            match Seq.tryHead currentCommandNamesQueue with
            | Some currentCommand ->
                if printConfigureChecks then
                    Console.Write(sprintf "checking for %s... " currentCommand)

                if not(Process.CommandWorksInShell currentCommand) then
                    if printConfigureChecks then
                        Console.WriteLine "not found"

                    configCommandCheck
                        (Seq.tail currentCommandNamesQueue)
                        allCommands
                else
                    if printConfigureChecks then
                        Console.WriteLine "found"

                    currentCommand |> Some
            | None ->
                if exitIfNotFound then
                    Console.Error.WriteLine(
                        sprintf
                            "Error, please install %s"
                            (String.Join(" or ", List.ofSeq allCommands))
                    )

                    Environment.Exit 1
                    failwith "unreachable"
                else
                    None

        configCommandCheck
            commandNamesByOrderOfPreference
            commandNamesByOrderOfPreference

    // FIXME: it returns the first result, but we should return all (array<string>)
    static member VsWhere(searchPattern: string) : Option<string> =
        if Misc.GuessPlatform() <> Misc.Platform.Windows then
            failwith "vswhere.exe doesn't exist in other platforms than Windows"

        let programFiles =
            Environment.GetFolderPath Environment.SpecialFolder.ProgramFilesX86

        let vswhereExe =
            Path.Combine(
                programFiles,
                "Microsoft Visual Studio",
                "Installer",
                "vswhere.exe"
            )
            |> FileInfo

        Process.ConfigCommandCheck
            (List.singleton vswhereExe.FullName)
            true
            false
        |> ignore

        let vswhereCmd =
            {
                Process.ProcessDetails.Command = vswhereExe.FullName
                Process.ProcessDetails.Arguments =
                    sprintf "-find %s" searchPattern
            }

        let procResult = Process.Execute(vswhereCmd, Process.Echo.Off)

        let entries =
            procResult
                .UnwrapDefault()
                .Split(
                    Array.singleton Environment.NewLine,
                    StringSplitOptions.RemoveEmptyEntries
                )

        if entries.Any() then
            entries.First().Trim() |> Some
        else
            None
