package flagforge.domain

import io.circe.derivation.Configuration

/** JSON conventions for the whole domain:
  *   - missing fields fall back to case class defaults;
  *   - sum types are flat objects with a `"type"` discriminator in snake_case (`{"type": "rollout", ...}`);
  *   - plain enums are encoded as snake_case strings (`"starts_with"`).
  */
given jsonConfiguration: Configuration =
  Configuration.default.withDefaults
    .withDiscriminator("type")
    .withSnakeCaseConstructorNames
