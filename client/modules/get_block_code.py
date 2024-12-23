def convert_block_to_text(block_content: str, block_type: str) -> str:
    block_content = block_content.strip()
    block_start = f'```{block_type}'
    block_end = '```'
    
    if block_content.startswith(block_start) and block_content.endswith(block_end):
        block_content = block_content[len(block_start):-len(block_end)]
        return block_content
    return block_content