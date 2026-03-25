# PAL MCP: Multi-Model Consensus Server

<div align="center">

  <em>Your AI's PAL – a Provider Abstraction Layer</em><br />
  <sub><a href="docs/name-change.md">Formerly known as Zen MCP</a></sub>

  [PAL in action](https://github.com/user-attachments/assets/0d26061e-5f21-4ab1-b7d0-f883ddc2c3da)

### Your CLI + Multiple Models = Expert Consensus

**Use the CLI you love:**
[Claude Code](https://www.anthropic.com/claude-code) · [Gemini CLI](https://github.com/google-gemini/gemini-cli) · [Codex CLI](https://github.com/openai/codex) · [Qwen Code CLI](https://qwenlm.github.io/qwen-code-docs/) · [Cursor](https://cursor.com) · _and more_

**Get consensus from multiple models in a single prompt:**
Gemini · OpenAI · Anthropic · Grok · Azure · Ollama · OpenRouter · DIAL · On-Device Model

</div>

---

## Why PAL MCP?

**Why rely on one AI model when you can get consensus from them all?**

A Model Context Protocol server that supercharges tools like [Claude Code](https://www.anthropic.com/claude-code), [Codex CLI](https://developers.openai.com/codex/cli), and IDE clients such
as [Cursor](https://cursor.com) or the [Claude Dev VS Code extension](https://marketplace.visualstudio.com/items?itemName=Anthropic.claude-vscode). **PAL MCP connects your favorite AI tool
to multiple AI models** for multi-model consensus analysis and collaborative decision-making.

### Multi-Model Consensus with Conversation Continuity

PAL's **consensus** tool queries multiple AI models simultaneously, collects their independent analyses, and synthesizes a unified recommendation with agreement/disagreement tracking. Conversation threading ensures context carries forward across rounds, enabling iterative deep-dives.

> **You're in control.** Your CLI of choice orchestrates the AI team, but you decide the workflow. Craft powerful prompts that bring in Gemini Pro, GPT 5, Flash, or local offline models exactly when needed.

<details>
<summary><b>Reasons to Use PAL MCP</b></summary>

1. **Multi-Model Consensus** - Get independent opinions from Gemini Pro, O3, GPT-5, and 50+ other models, then synthesize into a unified recommendation

2. **Stance Steering** - Ask specific models to argue for or against a proposal to stress-test ideas before committing

3. **Context Revival Magic** - Even after your CLI's context resets, continue conversations seamlessly by having other models "remind" it of the discussion

4. **Extended Context Windows** - Delegate to Gemini (1M tokens) or O3 (200K tokens) for massive codebases

5. **Conversation Continuity** - Full context flows across rounds - Gemini remembers what O3 said 10 steps ago

6. **Model-Specific Strengths** - Extended thinking with Gemini Pro, blazing speed with Flash, strong reasoning with O3, privacy with local Ollama

7. **Automatic Model Selection** - Your CLI intelligently picks the right models (or you can specify)

8. **Local Model Support** - Run Llama, Mistral, or other models locally for complete privacy and zero API costs

9. **SQLite Persistence** - Conversations survive server restarts and can be shared across instances

</details>

#### Recommended AI Stack

<details>
<summary>For Claude Code Users</summary>

For best results when using [Claude Code](https://claude.ai/code):

- **Sonnet 4.5** - All agentic work and orchestration
- **Gemini 3.0 Pro** OR **GPT-5.2 / Pro** - Deep analysis and consensus validation
</details>

<details>
<summary>For Codex Users</summary>

For best results when using [Codex CLI](https://developers.openai.com/codex/cli):

- **GPT-5.2 Codex Medium** - All agentic work and orchestration
- **Gemini 3.0 Pro** OR **GPT-5.2-Pro** - Deep analysis and consensus validation
</details>

## Quick Start (5 minutes)

**Prerequisites:** Python 3.10+, Git, [uv installed](https://docs.astral.sh/uv/getting-started/installation/)

**1. Get API Keys** (choose one or more):
- **[OpenRouter](https://openrouter.ai/)** - Access multiple models with one API
- **[Gemini](https://makersuite.google.com/app/apikey)** - Google's latest models
- **[OpenAI](https://platform.openai.com/api-keys)** - O3, GPT-5 series
- **[Azure OpenAI](https://learn.microsoft.com/azure/ai-services/openai/)** - Enterprise deployments of GPT-4o, GPT-4.1, GPT-5 family
- **[X.AI](https://console.x.ai/)** - Grok models
- **[DIAL](https://dialx.ai/)** - Vendor-agnostic model access
- **[Ollama](https://ollama.ai/)** - Local models (free)

**2. Install** (choose one):

**Option A: Clone and Automatic Setup** (recommended)
```bash
git clone https://github.com/BeehiveInnovations/pal-mcp-server.git
cd pal-mcp-server

# Handles everything: setup, config, API keys from system environment.
# Auto-configures Claude Desktop, Claude Code, Gemini CLI, Codex CLI, Qwen CLI
# Enable / disable additional settings in .env
./run-server.sh
```

**Option B: Instant Setup with [uvx](https://docs.astral.sh/uv/getting-started/installation/)**
```json
// Add to ~/.claude/settings.json or .mcp.json
// Don't forget to add your API keys under env
{
  "mcpServers": {
    "pal": {
      "command": "bash",
      "args": ["-c", "for p in $(which uvx 2>/dev/null) $HOME/.local/bin/uvx /opt/homebrew/bin/uvx /usr/local/bin/uvx uvx; do [ -x \"$p\" ] && exec \"$p\" --from git+https://github.com/BeehiveInnovations/pal-mcp-server.git pal-mcp-server; done; echo 'uvx not found' >&2; exit 1"],
      "env": {
        "PATH": "/usr/local/bin:/usr/bin:/bin:/opt/homebrew/bin:~/.local/bin",
        "GEMINI_API_KEY": "your-key-here",
        "DEFAULT_MODEL": "auto"
      }
    }
  }
}
```

**3. Start Using!**
```
"Get consensus from gemini pro and o3 on whether to use Redis or Memcached for our session cache"
"Use consensus to evaluate this architecture proposal with multiple models"
"Plan the migration strategy with pal, get consensus from pro and o3 on the approach"
```

👉 **[Complete Setup Guide](docs/getting-started.md)** with detailed installation, configuration for Gemini / Codex / Qwen, and troubleshooting
👉 **[Cursor & VS Code Setup](docs/getting-started.md#ide-clients)** for IDE integration instructions

## Provider Configuration

PAL activates any provider that has credentials in your `.env`. See `.env.example` for deeper customization.

## Tools

| Tool | Description |
|------|-------------|
| **[`consensus`](docs/tools/consensus.md)** | Multi-model analysis with stance steering (for/against/neutral). Queries multiple models, collects independent opinions, synthesizes unified recommendations. |
| **`listmodels`** | List all available AI models across configured providers |
| **`version`** | Display server version and system information |

## 📺 Watch Consensus In Action

<details>
<summary><b>Consensus Tool</b> - Multi-model debate and decision making</summary>

**Multi-model consensus debate:**

[PAL Consensus Debate](https://github.com/user-attachments/assets/76a23dd5-887a-4382-9cf0-642f5cf6219e)

</details>

## Key Features

**AI Orchestration**
- **Auto model selection** - Your CLI picks the right AI for each task
- **Multi-model consensus** - Get independent opinions and synthesized recommendations
- **Conversation continuity** - Context preserved across rounds and models
- **[Context revival](docs/context-revival.md)** - Continue conversations even after context resets

**Model Support**
- **Multiple providers** - Gemini, OpenAI, Azure, X.AI, OpenRouter, DIAL, Ollama
- **Latest models** - GPT-5, Gemini 3.0 Pro, O3, Grok-4, local Llama
- **Vision support** - Analyze images, diagrams, screenshots

**Storage & Persistence**
- **SQLite backend** - Conversations persist across server restarts (WAL mode)
- **In-memory fallback** - Set `PAL_STORAGE_BACKEND=memory` for ephemeral sessions
- **Cross-instance sharing** - Multiple MCP server instances can share the same conversation DB

## Example Workflows

**Multi-Model Architecture Decision:**
```
"Get consensus from gemini pro and o3 on whether to use microservices or a modular monolith"
```
→ Gemini Pro analyzes → O3 provides perspective → Synthesized recommendation with agreement tracking

**Stance-Steered Debate:**
```
"Use consensus with gpt-5 arguing FOR and gemini-pro arguing AGAINST migrating to Kubernetes"
```
→ Controlled debate → Both sides presented → Balanced synthesis

**Iterative Deep-Dive:**
```
"Continue the consensus discussion - now focus on the cost implications"
```
→ Previous context preserved → All models reference earlier analysis → Refined recommendations

👉 **[Advanced Usage Guide](docs/advanced-usage.md)** for complex workflows, model configuration, and power-user features

## Quick Links

**📖 Documentation**
- [Docs Overview](docs/index.md) - High-level map of major guides
- [Getting Started](docs/getting-started.md) - Complete setup guide
- [Tools Reference](docs/tools/) - Tool documentation with examples
- [Advanced Usage](docs/advanced-usage.md) - Power user features
- [Configuration](docs/configuration.md) - Environment variables, restrictions
- [Adding Providers](docs/adding_providers.md) - Provider-specific setup (OpenAI, Azure, custom gateways)
- [Model Ranking Guide](docs/model_ranking.md) - How intelligence scores drive auto-mode suggestions

**🔧 Setup & Support**
- [WSL Setup](docs/wsl-setup.md) - Windows users
- [Troubleshooting](docs/troubleshooting.md) - Common issues
- [Contributing](docs/contributions.md) - Code standards, PR process

## License

Apache 2.0 License - see [LICENSE](LICENSE) file for details.

## Acknowledgments

Built with the power of **Multi-Model AI** collaboration
- [MCP (Model Context Protocol)](https://modelcontextprotocol.com)
- [Codex CLI](https://developers.openai.com/codex/cli)
- [Claude Code](https://claude.ai/code)
- [Gemini](https://ai.google.dev/)
- [OpenAI](https://openai.com/)
- [Azure OpenAI](https://learn.microsoft.com/azure/ai-services/openai/)

### Star History

[![Star History Chart](https://api.star-history.com/svg?repos=BeehiveInnovations/pal-mcp-server&type=Date)](https://www.star-history.com/#BeehiveInnovations/pal-mcp-server&Date)
