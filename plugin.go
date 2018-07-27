package gorm

// PluginInterface plugin interface
type PluginInterface interface {
	Apply(*DB)
}

// Use use plugin
func (db *DB) Use(plugin PluginInterface) {
	plugin.Apply(db.parent)
}
